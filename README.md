
# Harper Foundation

A Razorpay payment service developed using Django, PSQL.



## Project Setup

Clone the repository: 
```bash
  git clone https://github.com/Jainpriyansh08/HarperFoundation.git
```

Create a python3.11 virtual environment and activate it:
```bash
  cd ezpay
  python3.11 -m venv env
  source env/bin/activate
```
Install all the requirements: 
```bash
  pip install requirements.txt
```
Adding environment variables and setting them:\
Create a file named local.env and add the follwing variables\

```bash
  set -a && source local.env && set +a
```
Apply the migrations:
```bash
  python manage.py migrate
```
Start the server:
```bash
  python manage.py runserver
```