# src/models.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    JSON,
    Index,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False, index=True
    )

    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    quiz_results: Mapped[list["QuizResult"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    portfolios: Mapped[list["Portfolio"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class QuizResult(Base):
    __tablename__ = "quiz_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    risk_class: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)

    neighbor_class: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    neighbor_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    profile_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    user: Mapped["User"] = relationship(back_populates="quiz_results")


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    telegram_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    risk_profile: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    method: Mapped[str] = mapped_column(String(50), nullable=False)      # max_sharpe / max_return / etc
    lookback: Mapped[int] = mapped_column(Integer, nullable=False, default=252)

    params_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    weights: Mapped[list["PortfolioWeight"]] = relationship(
        back_populates="portfolio",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    user: Mapped["User"] = relationship(back_populates="portfolios")

Index("ix_portfolios_telegram_created", Portfolio.telegram_id, Portfolio.created_at)


class PortfolioWeight(Base):
    __tablename__ = "portfolio_weights"

    portfolio_id: Mapped[int] = mapped_column(
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        primary_key=True,
    )
    instrument_uid: Mapped[str] = mapped_column(String(128), primary_key=True)

    secid: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    boardid: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    weight: Mapped[float] = mapped_column(Float, nullable=False)

    portfolio: Mapped["Portfolio"] = relationship(back_populates="weights")
