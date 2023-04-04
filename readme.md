# Pub/Sub Term Project Artificial Intelligence ADÜ
## Yusuf Metin ÖZER 221805073
### Homework Explanation
We have 3 publisher agents and 1 subscriber agent. Publishers use [`T1.csv`](https://github.com/ymozer/pubsub_term/blob/main/T1.csv) to stream. Then subscriber combines these streams and creates a file if they have matching time.
Data streamed in 1 second period.
### Installation

Firstly install Docker to computer and start application. After enter below command to terminal:

```bash
docker-compose up
```

From now on, we done for docker... After cloning repository:

```ps
cd pubsub_term
python -m venv .
./Scripts/Activate.ps1 # For Powershell
pip install -r ./requirements.txt
```
___
### Execution
While docker running, open two seperate terminal windows. Then run `publisher.py` and `subs.py`:
```ps
python publisher.py 
``` 
```ps
python subs.py 
``` 
