from dotenv import load_dotenv; import os; load_dotenv(); print('TOKEN:', os.getenv('TELEGRAM_BOT_TOKEN')); print('ORCH_URL:', os.getenv('ORCH_URL'))
