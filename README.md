# HarperFoundation

![build](https://github.com/betterhalf-ai/betterhalf-backend/workflows/build/badge.svg)
[![codecov](https://tinyurl.com/y34jamnv)](https://codecov.io/gh/betterhalf-ai/betterhalf-backend)

# Running tests 

Pre-requisites


Step 1: Clone the repository

Step 2: Copy the below mentioned environment files
```

```

Step 2:
```
touch local.env
cp testing.env docker-dev.env
```

Step 3: Create containers required for test
```
docker-compose --profile test up -d 
```
If you face issues with "--profile" command then just use `docker-compose up -d` 

Step 4: Install all required packages 
```
pip install -r requirements.txt
pip install -r requirements/base.txt

```

Step 4: Export environment variables for pytest to use 
```
export $(cat testing.env | xargs)
```

Now you can use pytest to run tests, for example
```
pytest 
pytest accounts
pytest accounts/tests/views/users/test_update_user.py                    
```


