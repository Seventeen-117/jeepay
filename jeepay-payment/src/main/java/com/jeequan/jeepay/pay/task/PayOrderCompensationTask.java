/*
 * Copyright (c) 2021-2031, 河北计全科技有限公司 (https://www.jeequan.com & jeequan@126.com).
 * <p>
 * Licensed under the GNU LESSER GENERAL PUBLIC LICENSE 3.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.gnu.org/licenses/lgpl.html
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jeequan.jeepay.pay.task;

import com.jeequan.jeepay.components.mq.model.PayOrderReissueMQ;
import com.jeequan.jeepay.components.mq.vender.IMQSender;
import com.jeequan.jeepay.core.constants.CS;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.entity.PayOrderCompensation;
import com.jeequan.jeepay.pay.channel.IPayOrderQueryService;
import com.jeequan.jeepay.pay.model.MchAppConfigContext;
import com.jeequan.jeepay.pay.rqrs.msg.ChannelRetMsg;
import com.jeequan.jeepay.pay.service.ConfigContextQueryService;
import com.jeequan.jeepay.pay.service.PayOrderProcessService;
import com.jeequan.jeepay.service.impl.PayOrderCompensationService;
import com.jeequan.jeepay.service.impl.PayOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

/**
 * 支付订单补偿任务
 * 处理SAGA事务补偿
 *
 * @author jeepay
 * @site https://www.jeequan.com
 * @date 2023/8/12
 */
@Slf4j
@Component
public class PayOrderCompensationTask {

    @Autowired private PayOrderCompensationService payOrderCompensationService;
    @Autowired private PayOrderService payOrderService;
    @Autowired private ConfigContextQueryService configContextQueryService;
    @Autowired private PayOrderProcessService payOrderProcessService;
    @Autowired private IMQSender mqSender;

    /**
     * 每5分钟执行一次，处理未完成的补偿记录
     */
    @Scheduled(fixedDelay = 300000)
    public void processCompensationRecords() {
        log.info("开始处理支付订单补偿记录...");
        
        try {
            // 查询处理中的补偿记录
            List<PayOrderCompensation> compensationList = payOrderCompensationService.list(
                    PayOrderCompensation.gw()
                            .eq(PayOrderCompensation::getState, 0) // 处理中状态
                            .lt(PayOrderCompensation::getCreatedAt, new Date(System.currentTimeMillis() - 60000)) // 创建时间超过1分钟
            );
            
            if (compensationList == null || compensationList.isEmpty()) {
                return;
            }
            
            log.info("找到{}条待处理的补偿记录", compensationList.size());
            
            // 处理每条补偿记录
            for (PayOrderCompensation compensation : compensationList) {
                processCompensation(compensation);
            }
            
        } catch (Exception e) {
            log.error("处理支付订单补偿记录异常", e);
        }
    }
    
    /**
     * 处理单条补偿记录
     */
    private void processCompensation(PayOrderCompensation compensation) {
        try {
            String payOrderId = compensation.getPayOrderId();
            
            // 查询订单
            PayOrder payOrder = payOrderService.getById(payOrderId);
            if (payOrder == null) {
                log.warn("补偿记录对应的订单不存在，payOrderId={}", payOrderId);
                updateCompensationFailed(compensation, "订单不存在");
                return;
            }
            
            // 如果订单已经成功或失败，更新补偿记录状态
            if (payOrder.getState() == PayOrder.STATE_SUCCESS) {
                updateCompensationSuccess(compensation, "订单已支付成功");
                return;
            } else if (payOrder.getState() == PayOrder.STATE_FAIL || payOrder.getState() == PayOrder.STATE_CANCEL) {
                updateCompensationFailed(compensation, "订单已失败或取消");
                return;
            }
            
            // 获取商户应用配置
            MchAppConfigContext mchAppConfigContext = configContextQueryService.queryMchInfoAndAppInfo(
                    payOrder.getMchNo(), payOrder.getAppId());
            
            if (mchAppConfigContext == null) {
                log.warn("获取商户应用配置信息失败，payOrderId={}", payOrderId);
                updateCompensationFailed(compensation, "获取商户应用配置信息失败");
                return;
            }
            
            // 查询补偿渠道的支付结果
            String compensationIfCode = compensation.getCompensationIfCode();
            IPayOrderQueryService queryService = getPayOrderQueryService(compensationIfCode);
            
            if (queryService == null) {
                log.warn("补偿渠道查询服务不存在，ifCode={}", compensationIfCode);
                updateCompensationFailed(compensation, "补偿渠道查询服务不存在");
                return;
            }
            
            // 查询支付结果
            ChannelRetMsg channelRetMsg = queryService.query(payOrder, mchAppConfigContext);
            
            // 处理查询结果
            if (channelRetMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_SUCCESS) {
                // 支付成功，更新订单状态
                payOrderProcessService.confirmSuccess(payOrder);
                updateCompensationSuccess(compensation, "补偿渠道支付成功");
                
            } else if (channelRetMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_FAIL) {
                // 支付失败，更新订单状态
                payOrderService.updateIng2FailByOrderId(payOrderId);
                updateCompensationFailed(compensation, "补偿渠道支付失败");
                
            } else {
                // 支付中，继续等待
                log.info("补偿渠道支付处理中，稍后再查询，payOrderId={}", payOrderId);
                
                // 再次发送延迟消息，继续查询
                mqSender.send(PayOrderReissueMQ.build(payOrderId, 1), 60);
            }
            
        } catch (Exception e) {
            log.error("处理补偿记录异常，compensationId={}", compensation.getCompensationId(), e);
        }
    }
    
    /**
     * 更新补偿记录为成功
     */
    private void updateCompensationSuccess(PayOrderCompensation compensation, String resultInfo) {
        payOrderCompensationService.updateCompensationState(
                compensation.getCompensationId(), (byte)1, resultInfo);
    }
    
    /**
     * 更新补偿记录为失败
     */
    private void updateCompensationFailed(PayOrderCompensation compensation, String resultInfo) {
        payOrderCompensationService.updateCompensationState(
                compensation.getCompensationId(), (byte)2, resultInfo);
    }
    
    /**
     * 获取支付订单查询服务
     */
    private IPayOrderQueryService getPayOrderQueryService(String ifCode) {
        try {
            return (IPayOrderQueryService) Class.forName("com.jeequan.jeepay.pay.channel." + 
                    ifCode.toLowerCase() + "." + ifCode + "PayOrderQueryService").newInstance();
        } catch (Exception e) {
            log.error("获取支付订单查询服务失败，ifCode={}", ifCode, e);
            return null;
        }
    }
} 