import re
import datetime
import markovify
import random

PROCESS = 4
# 进程数
LENGTH = 100000
# 产生 event 数量
MARKOV_STATE_COUNT = 3
# 用连续多少个状态来预测下一个状态
FUNC_PARA_MAP = {
    'MPI_Send': ('buf', 'count', 'datatype', 'dest', 'tag', 'comm'),
    'MPI_Recv': ('buf', 'count', 'datatype', 'source', 'tag', 'comm', 'status'),
    'MPI_Isend': ('buf', 'count', 'datatype', 'dest', 'tag', 'comm', 'request'),
    'MPI_Irecv': ('buf', 'count', 'datatype', 'source', 'tag', 'comm', 'request'),
    'MPI_Wait': ('request', 'status'),
    'MPI_Waitall': ('count', 'array_of_requests', 'array_of_statuses'),
    'MPI_Bcast': ('buffer', 'count', 'datatype', 'root', 'comm'),
    'MPI_Alltoall': ('sendbuf', 'sendcount', 'sendtype', 'recvbuf', 'recvcount'),
    'MPI_Alltoallv': ('sendbuf', 'sendcounts', 'sdispls', 'sendtype', 'recvbuf', 'recvcount'),
    'MPI_Reduce': ('sendbuf', 'recvbuf', 'count', 'datatype', 'op', 'root', 'comm'),
    'MPI_Allreduce': ('sendbuf', 'recvbuf', 'count', 'datatype', 'op', 'comm'),
}

traces = []
events = []

for i in range(PROCESS):
    f = open('data/trace.{}'.format(i))
    traces.append(f)

rank = 0
for file in traces:
    for line in file.readlines():
        try:
            event = {'parameters': re.findall(r'[(](.*?)[)]', line)[0],
                     'start_time': datetime.datetime.strptime('2020-11-19 ' + line.split(' ')[0],
                                                              '%Y-%m-%d %H:%M:%S.%f'),
                     'function': re.findall(r'(?<= ).*(?=\()', line)[0],
                     'duration': re.findall(r'(?<=<).*(?=>)', line)[0],
                     'rank': rank
                     }
            if event['function'].startswith('MPI'):
                events.append(event)
        except Exception as e:
            continue
            # print(line)
            # print(e)
    rank += 1

events.sort(key=lambda x: x['start_time'])
functions_duration = {}
for event in events:
    if event['function'] in functions_duration:
        functions_duration[event['function']] = event['duration']

functions_with_rank = ['{0}_{1}'.format(event['function'], event['rank']) for event in events]
output_file = open('data/output.csv', 'w+')
output_file.write('function, rank, start_time, duration')
for event in events:
    output_file.write(
        '{0}, {1}, {2}, {3}\n'.format(event['function'], event['rank'],
                                      int(event['start_time'].timestamp()) * 1000000 + event['start_time'].microsecond,
                                      int(float(event['duration']) * 1000000)))

output_file.close()
model = markovify.Chain([functions_with_rank[:-50]], PROCESS - 1)
model.compile()
tries = 0
status = []
state = ('___BEGIN__',) * (PROCESS - 1)
for i in range(LENGTH):
    while True:
        next_state = model.move(state)
        if next_state != '___END__':
            break
    status.append(next_state)
    state = tuple(state[1:]) + (next_state,)

count = 1
start_times = [0] * PROCESS
function_name_index = {}

output_files = [open('data/{}.txt'.format(i), 'w+') for i in range(PROCESS)]
for event in events:
    func = '{0}_{1}'.format(event['function'], event['rank'])
    if func in function_name_index:
        function_name_index[func].append(event)
    else:
        function_name_index[func] = [event]

for state in status:
    filter_events = function_name_index[state]
    event = filter_events[random.randint(0, len(filter_events) - 1)]
    rank = event['rank']
    output_files[rank].write(str(count) + ': ')
    output_files[rank].write(event['function'] + '= ')
    params = event['parameters'].split(' ')
    output_files[rank].write('(')
    if event['function'] in FUNC_PARA_MAP:
        for i in range(len(params)):
            output_files[rank].write(FUNC_PARA_MAP[event['function']][i])
            output_files[rank].write(' = ')
            output_files[rank].write(params[i])
    else:
        output_files[rank].write(event['parameters'])
    output_files[rank].write(') ')
    output_files[rank].write('S=')
    output_files[rank].write('[' + str(start_times[event['rank']]) + '] ')
    start_times[event['rank']] += int(float(event['duration']) * 1000000)
    output_files[rank].write('E=')
    output_files[rank].write('[' + str(start_times[event['rank']]) + '] ')
    output_files[rank].write('D=')
    output_files[rank].write('[' + str(int(float(event['duration']) * 1000000)) + ']\n')
    count += 1

for file in output_files:
    file.close()
