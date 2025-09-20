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
    
    async def get_by_receipt_id(self, receipt_id: str):
        """
        Возвращает PaymentReceipt по ReceiptId (может быть None, если не найден).
        """
        from sqlalchemy import select
        from db.models import PaymentReceipt  # если не импортирован вверху файла
        q = await self.session.execute(
            select(PaymentReceipt).where(PaymentReceipt.receipt_id == receipt_id)
        )
        return q.scalars().first()
    
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

    async def mark_sent(self, pr, receipt_id: str, new_invoice_id: str | None = None):
        """
        Обновляет статус на SENT, сохраняет ReceiptId и при необходимости обновляет invoice_id,
        если Ferma вернула свой InvoiceId.
        """
        pr.status = ReceiptStatus.SENT
        pr.receipt_id = receipt_id
        if new_invoice_id:
            pr.invoice_id = new_invoice_id
        await self.session.flush()
        return pr


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