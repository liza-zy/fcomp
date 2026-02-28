# src/models.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from datetime import datetime, timezone

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, index=True)

    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    quiz_results: Mapped[list["QuizResult"]] = relationship(back_populates="user")


class QuizResult(Base):
    __tablename__ = "quiz_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    risk_class: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)

    neighbor_class: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    neighbor_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    profile_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="quiz_results")

class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, nullable=False, index=True)

    risk_profile = Column(String(50), nullable=True)
    method = Column(String(50), nullable=False)         # "max_sharpe", "max_return", etc
    lookback = Column(Integer, nullable=False, default=252)

    # опционально: сохраняем параметры запроса (сектора, исключения, budget)
    params_json = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    weights = relationship(
        "PortfolioWeight",
        back_populates="portfolio",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    Index("ix_portfolios_telegram_created", Portfolio.telegram_id, Portfolio.created_at)


class PortfolioWeight(Base):
    __tablename__ = "portfolio_weights"

    portfolio_id = Column(Integer, ForeignKey("portfolios.id", ondelete="CASCADE"), primary_key=True)
    instrument_uid = Column(String(128), primary_key=True)

    secid = Column(String(32), nullable=True)
    boardid = Column(String(16), nullable=True)
    asset_class = Column(String(16), nullable=True)

    weight = Column(Numeric(18, 10), nullable=False)  # точнее чем float

    portfolio = relationship("Portfolio", back_populates="weights")