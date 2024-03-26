package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.subscribers.OrderPaymentSubscriber
import java.util.UUID

@Service
abstract class DefaultPaymentServiceSelector {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

        fun processingPaymentOperation(
            paymentServices: List<PaymentService>,
            isReset : Boolean,
            getNearest : Int,
            nearestTimes: LongArray,
            event: OrderPaymentStartedEvent,
            createdEvent: PaymentCreatedEvent,
            ){
            outerCycle@ while(true) {
                if (!isReset) {
                    val index = getNearest
                    if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                        if (paymentServices[index].rateLimiter.tick()) {
                            nearestTimes[index] =
                                (System.currentTimeMillis() + (1.0 / paymentServices[index].calculateSpeed())).toLong()
                            paymentServices[index].submitPaymentRequest(
                                createdEvent.paymentId,
                                event.amount,
                                event.createdAt
                            )
                            break
                        } else {
                            paymentServices[index].window.releaseWindow()
                        }
                    }
                } else
                    for (index in paymentServices.indices) {
                        if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                            if (paymentServices[index].rateLimiter.tick()) {
                                nearestTimes[index] =
                                    (System.currentTimeMillis() + (1.0 / paymentServices[index].calculateSpeed())).toLong()
                                paymentServices[index].submitPaymentRequest(
                                    createdEvent.paymentId,
                                    event.amount,
                                    event.createdAt
                                )
                                break@outerCycle
                            } else {
                                paymentServices[index].window.releaseWindow()
                            }
                        }
                    }
            }

        }
    }
}