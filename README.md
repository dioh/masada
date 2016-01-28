# MASADA: Modeling and Simulation Automated Data Analysis

## Instalation
Now install is required whatsoever be sure you meet dependencies. Check pip freeze file requirements.txt or install them by doing:

```
pip install --user -r requirements.txt 

```

--user flag so no privileges are required and, if with your home in AFS, no need to re-install them from machine to machine.

## How to use

Usage example:

```
./masada.py retrieve 284000 --stime 1448136000 --etime 1448312400 --dataset RunDS
./masada.py retrieve 284000 --lumiblocks 40 --length 30 --dataset AllLumiblocksDS
./masada.py retrieve 284000 --dataset AcceptedEventLatencyDS --multiple
./masada.py retrieve 284000 --dataset  TrafficShappingCreditsDS --multiple
```



