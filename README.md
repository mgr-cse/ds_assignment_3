# ds_assignment_2

Distributed Queue assignment

## Code structure


## Prerequisites


## Installing prerequisites

Install the required python system packages: 
```bash
sudo apt install python3-venv python3-pip postgresql
```
### Setting up repository
```bash
git clone https://github.com/mgr-cse/ds_assignment_2
cd ds_assignment_1
python3 -m venv 01-env
source env/bin/activate
pip install -r requirements.txt
```
### Setting up database
```bash
sudo systemctl start postgresql
sudo -iu postgres psql < create_database.sql
```
## Running tests


### Sample test output


    