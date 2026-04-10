@echo off
cd /d C:\Users\ericn\PokeBot

echo === Record Online Drop ===

set /p PRICE=Enter observed price: 
set /p PRODUCT_ID=Enter product ID: 
set /p NOTES=Any notes (optional): 

python -m agent.record_drop ^
  --retailer Walmart ^
  --source app ^
  --zip 00000 ^
  --price %PRICE% ^
  --product_ids %PRODUCT_ID% ^
  --notes "%NOTES%"

echo Drop recorded.
pause
