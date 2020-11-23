# MPI Events Generator

## Usage


```shell script
pip install -r requirements.txt
```

Collect traces.

```shell script
mpirun -np N sh ./wrapper {path}/executable
```

Then configure `PROCESS`, `LENGTH`, `MARKOV_STATE_COUNT` in generator.py

```shell script
python generator.py
```

`data/{x}.txt`s are output files.