@echo off
echo Installing Product Quality Control Agent Dependencies...
echo =====================================================

echo Checking Python installation...
python --version
if %errorlevel% neq 0 (
    echo ERROR: Python not found! Please install Python first.
    pause
    exit /b 1
)

echo Upgrading pip...
python -m pip install --upgrade pip

echo Installing dependencies from requirements.txt...
python -m pip install -r requirements.txt

echo =====================================================
echo Installation complete!
echo You can now run: streamlit run app/streamlit_app.py
pause