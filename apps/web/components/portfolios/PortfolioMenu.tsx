'use client';

import { useState } from 'react';

type Props = {
  portfolioName: string;
  onRename: (nextName: string) => Promise<void> | void;
  onDelete: () => Promise<void> | void;
  disabled?: boolean;
};

export default function PortfolioMenu({
  portfolioName,
  onRename,
  onDelete,
  disabled = false,
}: Props) {
  const [open, setOpen] = useState(false);
  const [isRenaming, setIsRenaming] = useState(false);

  const handleRename = async () => {
    const nextName = window.prompt('Новое название портфеля', portfolioName)?.trim();

    if (!nextName || nextName === portfolioName) {
      return;
    }

    setIsRenaming(true);
    try {
      await onRename(nextName);
      setOpen(false);
    } finally {
      setIsRenaming(false);
    }
  };

  const handleDelete = async () => {
    const confirmed = window.confirm(`Удалить портфель "${portfolioName}"?`);

    if (!confirmed) {
      return;
    }

    await onDelete();
    setOpen(false);
  };

  return (
    <div className="relative">
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen((prev) => !prev)}
        className="flex h-10 w-10 items-center justify-center rounded-full border border-gray-200 bg-white text-lg text-gray-700 shadow-sm disabled:opacity-50"
        aria-label="Открыть меню портфеля"
      >
        ⋯
      </button>

      {open && (
        <div className="absolute right-0 top-12 z-20 min-w-[180px] overflow-hidden rounded-2xl border border-gray-200 bg-white shadow-lg">
          <button
            type="button"
            onClick={handleRename}
            disabled={isRenaming}
            className="block w-full px-4 py-3 text-left text-sm text-gray-800 hover:bg-gray-50 disabled:opacity-50"
          >
            Переименовать
          </button>

          <button
            type="button"
            onClick={handleDelete}
            className="block w-full px-4 py-3 text-left text-sm text-red-600 hover:bg-red-50"
          >
            Удалить
          </button>
        </div>
      )}
    </div>
  );
}
