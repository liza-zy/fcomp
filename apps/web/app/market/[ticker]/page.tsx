import AssetDetailsScreen from '@/components/screens/AssetDetailsScreen';

export default async function Page({
  params,
}: {
  params: Promise<{ ticker: string }>;
}) {
  const { ticker } = await params;

  return <AssetDetailsScreen ticker={ticker} />;
}
