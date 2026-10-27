echo Setting up virtual environment for filetransfer
python -m venv venv
call venv\Scripts\activate
echo Installing python libraries
pip install charset_normalizer-3.5.2-py3-none-any.whl
pip install idna-3.20-py3-none-any.whl 
pip install certifi-2026.10.23-py3-none-any.whl
pip install urllib3-2.8.0-py3-none-any.whl
pip install requests-2.34.2-py3-none-any.whl
pip install requests_toolbelt-1.0.0-py2.py3-none-any.whl
pip install psutil-7.2.2-cp37-abi3-win_amd64.whl

echo schtasks /create /sc ONLOGON /tn kantele_filetransfer /tr "python %cd%/transfer.bat" > tasksetup.bat
echo echo Created task in task scheduler >> tasksetup.bat
echo pause >> tasksetup.bat
echo Done setting up, please run tasksetup.bat as administrator 
pause

