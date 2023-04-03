# Pub/Sub Term Project Artificial Inteligance ADÜ
## Yusuf Metin ÖZER 221805073
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