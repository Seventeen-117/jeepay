/*
 * Copyright (c) 2021-2031, 江阳科技有限公司
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
package com.jeequan.jeepay.pay.ctrl.payorder;

import com.jeequan.jeepay.core.constants.CS;
import com.jeequan.jeepay.core.entity.PayOrder;
import com.jeequan.jeepay.core.entity.PayWay;
import com.jeequan.jeepay.core.exception.BizException;
import com.jeequan.jeepay.core.model.ApiRes;
import com.jeequan.jeepay.core.utils.JeepayKit;
import com.jeequan.jeepay.pay.rqrs.msg.ChannelRetMsg;
import com.jeequan.jeepay.pay.rqrs.payorder.UnifiedOrderRQ;
import com.jeequan.jeepay.pay.rqrs.payorder.UnifiedOrderRS;
import com.jeequan.jeepay.pay.rqrs.payorder.payway.AutoBarOrderRQ;
import com.jeequan.jeepay.pay.service.ConfigContextQueryService;
import com.jeequan.jeepay.pay.service.FallbackPaymentService;
import com.jeequan.jeepay.pay.service.PayOrderDistributedTransactionService;
import com.jeequan.jeepay.pay.model.MchAppConfigContext;
import com.jeequan.jeepay.service.impl.PayOrderService;
import com.jeequan.jeepay.service.impl.PayWayService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/*
* 统一下单 controller
*
* @author terrfly
* @site curverun.com
* @date 2021/6/8 17:27
*/
@Slf4j
@RestController
public class UnifiedOrderController extends AbstractPayOrderController {

    @Autowired private PayWayService payWayService;
    @Autowired private ConfigContextQueryService configContextQueryService;
    @Autowired private PayOrderDistributedTransactionService payOrderDistributedTransactionService;
    @Autowired private PayOrderService payOrderService;
    @Autowired private FallbackPaymentService fallbackPaymentService;

    /**
     * 统一下单接口
     * **/
    @PostMapping("/api/pay/unifiedOrder")
    public ApiRes unifiedOrder(){

        //获取参数 & 验签
        UnifiedOrderRQ rq = getRQByWithMchSign(UnifiedOrderRQ.class);

        UnifiedOrderRQ bizRQ = buildBizRQ(rq);

        //实现子类的res
        ApiRes apiRes = unifiedOrder(bizRQ.getWayCode(), bizRQ);
        if(apiRes.getData() == null){
            return apiRes;
        }

        UnifiedOrderRS bizRes = (UnifiedOrderRS)apiRes.getData();

        //聚合接口，返回的参数
        UnifiedOrderRS res = new UnifiedOrderRS();
        BeanUtils.copyProperties(bizRes, res);

        //只有 订单生成（QR_CASHIER） || 支付中 || 支付成功返回该数据
        if(bizRes.getOrderState() != null && (bizRes.getOrderState() == PayOrder.STATE_INIT || bizRes.getOrderState() == PayOrder.STATE_ING || bizRes.getOrderState() == PayOrder.STATE_SUCCESS) ){
            res.setPayDataType(bizRes.buildPayDataType());
            res.setPayData(bizRes.buildPayData());
        }

        return ApiRes.okWithSign(res, configContextQueryService.queryMchApp(rq.getMchNo(), rq.getAppId()).getAppSecret());
    }
    
    /**
     * 备用支付接口
     * 当主支付渠道失败时，可以手动触发备用支付通道
     **/
    @PostMapping("/api/pay/fallbackPayment/{payOrderId}")
    public ApiRes fallbackPayment(@PathVariable("payOrderId") String payOrderId, 
                                 @RequestParam(value = "wayCode", required = false) String wayCode) {
        try {
            // 查询订单信息
            PayOrder payOrder = payOrderService.getById(payOrderId);
            if (payOrder == null) {
                return ApiRes.customFail("订单不存在");
            }
            
            // 检查订单状态，只有初始化或处理中的订单才能进行备用支付
            if (payOrder.getState() != PayOrder.STATE_INIT && payOrder.getState() != PayOrder.STATE_ING) {
                return ApiRes.customFail("订单状态不允许进行备用支付");
            }
            
            // 如果未指定支付方式，则使用订单原支付方式
            if (wayCode == null || wayCode.isEmpty()) {
                wayCode = payOrder.getWayCode();
            }
            
            // 获取商户应用配置信息
            MchAppConfigContext mchAppConfigContext = configContextQueryService.queryMchInfoAndAppInfo(
                    payOrder.getMchNo(), payOrder.getAppId());
            if (mchAppConfigContext == null) {
                return ApiRes.customFail("获取商户应用信息失败");
            }
            
            // 构建支付请求对象
            UnifiedOrderRQ bizRQ = new UnifiedOrderRQ();
            bizRQ.setMchNo(payOrder.getMchNo());
            bizRQ.setAppId(payOrder.getAppId());
            bizRQ.setMchOrderNo(payOrder.getMchOrderNo());
            bizRQ.setWayCode(wayCode);
            bizRQ.setAmount(payOrder.getAmount());
            bizRQ.setCurrency(payOrder.getCurrency());
            bizRQ.setClientIp(payOrder.getClientIp());
            bizRQ.setSubject(payOrder.getSubject());
            bizRQ.setBody(payOrder.getBody());
            bizRQ.setNotifyUrl(payOrder.getNotifyUrl());
            bizRQ.setReturnUrl(payOrder.getReturnUrl());
            bizRQ.setExpiredTime(payOrder.getExpiredTime());
            
            // 调用备用支付服务
            ChannelRetMsg channelRetMsg = fallbackPaymentService.fallbackPayment(
                    wayCode, bizRQ, payOrder, mchAppConfigContext);
            
            // 处理返回结果
            if (channelRetMsg.getChannelState() == ChannelRetMsg.ChannelState.CONFIRM_SUCCESS) {
                return ApiRes.ok("备用支付成功");
            } else if (channelRetMsg.getChannelState() == ChannelRetMsg.ChannelState.WAITING) {
                return ApiRes.ok("备用支付处理中，请等待结果");
            } else {
                return ApiRes.customFail("备用支付失败: " + channelRetMsg.getChannelErrMsg());
            }
            
        } catch (BizException e) {
            return ApiRes.customFail(e.getMessage());
        } catch (Exception e) {
            log.error("备用支付异常：", e);
            return ApiRes.customFail("系统异常");
        }
    }

    private UnifiedOrderRQ buildBizRQ(UnifiedOrderRQ rq){

        //支付方式  比如： ali_bar
        String wayCode = rq.getWayCode();

        //jsapi 收银台聚合支付场景 (不校验是否存在payWayCode)
        if(CS.PAY_WAY_CODE.QR_CASHIER.equals(wayCode)){
            return rq.buildBizRQ();
        }

        //如果是自动分类条码
        if(CS.PAY_WAY_CODE.AUTO_BAR.equals(wayCode)){

            AutoBarOrderRQ bizRQ = (AutoBarOrderRQ)rq.buildBizRQ();
            wayCode = JeepayKit.getPayWayCodeByBarCode(bizRQ.getAuthCode());
            rq.setWayCode(wayCode.trim());
        }

        if(payWayService.count(PayWay.gw().eq(PayWay::getWayCode, wayCode)) <= 0){
            throw new BizException("不支持的支付方式");
        }

        //转换为 bizRQ
        return rq.buildBizRQ();
    }

    /**
     * 重写父类方法，使用分布式事务处理支付请求
     */
    @Override
    protected ApiRes unifiedOrder(String wayCode, UnifiedOrderRQ bizRQ, PayOrder payOrder) {
        
        try {
            // 获取商户应用配置信息
            MchAppConfigContext mchAppConfigContext = configContextQueryService.queryMchInfoAndAppInfo(bizRQ.getMchNo(), bizRQ.getAppId());
            if (mchAppConfigContext == null) {
                throw new BizException("获取商户应用信息失败");
            }
            
            // 使用分布式事务服务处理支付请求
            ChannelRetMsg channelRetMsg = payOrderDistributedTransactionService.handlePayTransaction(
                    wayCode, bizRQ, payOrder, mchAppConfigContext);
            
            // 使用父类的统一下单方法处理后续逻辑
            return super.unifiedOrder(wayCode, bizRQ, payOrder);
            
        } catch (BizException e) {
            return ApiRes.customFail(e.getMessage());
        } catch (Exception e) {
            log.error("统一下单异常：", e);
            return ApiRes.customFail("系统异常");
        }
    }
}
