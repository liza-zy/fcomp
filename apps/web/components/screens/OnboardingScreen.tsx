'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';

const slides = [
  {
    title: 'Умный анализ портфеля',
    description:
      'Получайте детальную аналитику инвестиций с персональными идеями по ребалансировке на основе ИИ.',
  },
  {
    title: 'Управление рисками',
    description:
      'Понимайте и контролируйте риски портфеля с учетом вашей склонности к риску.',
  },
  {
    title: 'Будьте в курсе',
    description:
      'Получайте актуальные новости рынка и события, влияющие на ваши инвестиции.',
  },
];

export default function OnboardingScreen() {
  const router = useRouter();
  const [currentSlide, setCurrentSlide] = useState(0);

  const isLast = currentSlide === slides.length - 1;

  const handleNext = () => {
    if (isLast) {
      router.push('/welcome');
      return;
    }
    setCurrentSlide((prev) => prev + 1);
  };

  const handleSkip = () => {
    router.push('/welcome');
  };

  return (
    <main className="min-h-screen bg-background flex flex-col justify-between p-6">
      <div className="flex justify-end">
        <button
          onClick={handleSkip}
          className="text-sm text-muted-foreground"
        >
          Пропустить
        </button>
      </div>

      <div className="flex-1 flex flex-col items-center justify-center text-center">
        <div className="mb-8 h-24 w-24 rounded-full bg-muted" />
        <h1 className="text-2xl font-semibold mb-4">{slides[currentSlide].title}</h1>
        <p className="text-muted-foreground max-w-sm">
          {slides[currentSlide].description}
        </p>
      </div>

      <div className="pb-6">
        <div className="flex justify-center gap-2 mb-6">
          {slides.map((_, idx) => (
            <div
              key={idx}
              className={`h-2 w-2 rounded-full ${
                idx === currentSlide ? 'bg-foreground' : 'bg-muted'
              }`}
            />
          ))}
        </div>

        <button
          onClick={handleNext}
          className="w-full rounded-xl px-4 py-3 bg-black text-white"
        >
          {isLast ? 'Начать' : 'Далее'}
        </button>
      </div>
    </main>
  );
}
