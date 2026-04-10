@echo off
cd /d C:\Users\ericn\PokeBot

echo === Record Target In-Person Drop ===

set /p PRICE=Enter observed price: 
set /p PRODUCT_ID=Enter product ID: 
set /p NOTES=Any notes (optional): 

python -m agent.record_drop ^
  --retailer Target ^
  --source in_person ^
  --zip 60491 ^
  --price %PRICE% ^
  --product_ids %PRODUCT_ID% ^
  --notes "%NOTES%"

echo Drop recorded.
pause