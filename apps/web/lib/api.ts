const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_URL || '';

export async function bootstrapUser(initData: string) {
  const response = await fetch(`${API_BASE_URL}/api/me/bootstrap`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      init_data: initData,
    }),
  });

  const text = await response.text();
  console.log('Bootstrap status:', response.status);
  console.log('Bootstrap response:', text);


  if (!response.ok) {
    throw new Error(`Bootstrap request failed: ${response.status}`);
  }

  return JSON.parse(text);
}
