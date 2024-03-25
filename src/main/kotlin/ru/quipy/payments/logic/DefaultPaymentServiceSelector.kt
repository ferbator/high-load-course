package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentCreatedEvent
import java.util.UUID

@Service
abstract class DefaultPaymentServiceSelector {
    companion object {
         fun selectPaymentService(
            event: OrderPaymentStartedEvent,
            createdEvent: PaymentCreatedEvent,
            secondPaymentService: PaymentService,
            firstPaymentService: PaymentService
        ) {
            if (!trySubmitPayment(secondPaymentService, createdEvent.paymentId, event.amount, event.createdAt)) {
                trySubmitPayment(firstPaymentService, createdEvent.paymentId, event.amount, event.createdAt)
            }
        }

        fun trySubmitPayment(
            paymentService: PaymentService,
            paymentId: UUID,
            amount: Int,
            paymentStartedAt: Long
        ): Boolean {
            if (paymentService.notOverTime(paymentStartedAt)) {
                val windowResponse = paymentService.window.putIntoWindow()
                if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                    if (paymentService.rateLimiter.tick()) {
                        paymentService.submitPaymentRequest(paymentId, amount, paymentStartedAt)
                        return true
                    } else {
                        paymentService.window.releaseWindow()
                    }
                }
            }
            return false
        }
    }
}