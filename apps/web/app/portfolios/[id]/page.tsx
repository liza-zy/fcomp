import PortfolioDetailScreen from '@/components/portfolios/PortfolioDetailScreen';

type Props = {
  params: Promise<{ id: string }>;
};

export default async function PortfolioDetailPage({ params }: Props) {
  const resolved = await params;
  const portfolioId = Number(resolved.id);

  if (!Number.isFinite(portfolioId)) {
    throw new Error('Invalid portfolio id');
  }

  return <PortfolioDetailScreen portfolioId={portfolioId} />;
}
