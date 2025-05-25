import datetime
import pytz
cdt = pytz.timezone('America/Chicago') # CDT timezone
now = datetime.datetime.now(cdt)

print(f'({now.hour}  {now.minute} )')