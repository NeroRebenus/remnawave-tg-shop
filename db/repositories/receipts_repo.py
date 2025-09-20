from __future__ import annotations
from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import PaymentReceipt, ReceiptStatus

class ReceiptsRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_payment_id(self, payment_id: str) -> Optional[PaymentReceipt]:
        res = await self.session.execute(select(PaymentReceipt).where(PaymentReceipt.payment_id == payment_id))
        return res.scalars().first()

    async def get_by_invoice_id(self, invoice_id: str) -> Optional[PaymentReceipt]:
        res = await self.session.execute(select(PaymentReceipt).where(PaymentReceipt.invoice_id == invoice_id))
        return res.scalars().first()

    async def create_new(self, *, payment_id: str, invoice_id: str, amount: float, description: str,
                         email: Optional[str], phone: Optional[str]) -> PaymentReceipt:
        pr = PaymentReceipt(
            payment_id=payment_id,
            invoice_id=invoice_id,
            amount=amount,
            description=description or "",
            customer_email=email,
            customer_phone=phone,
            status=ReceiptStatus.NEW,
        )
        self.session.add(pr)
        await self.session.flush()  # чтобы получить id
        return pr

    async def mark_sent(self, pr: PaymentReceipt, receipt_id: str):
        pr.receipt_id = receipt_id
        pr.status = ReceiptStatus.SENT
        pr.attempts += 1
        await self.session.flush()

    async def mark_confirmed(self, pr: PaymentReceipt, ofd_url: Optional[str]):
        pr.status = ReceiptStatus.CONFIRMED
        pr.ofd_receipt_url = ofd_url
        pr.last_error = None
        await self.session.flush()

    async def mark_processed(self, pr: PaymentReceipt):
        pr.status = ReceiptStatus.PROCESSED
        await self.session.flush()

    async def mark_failed(self, pr: PaymentReceipt, error: str):
        pr.status = ReceiptStatus.FAILED
        pr.last_error = error[:4000]
        await self.session.flush()

    async def mark_kkt_error(self, pr: PaymentReceipt, error: str):
        pr.status = ReceiptStatus.KKT_ERROR
        pr.last_error = error[:4000]
        await self.session.flush()

    async def mark_duplicate(self, pr: PaymentReceipt):
        pr.status = ReceiptStatus.DUPLICATE
        await self.session.flush()