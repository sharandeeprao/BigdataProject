import json
import subprocess
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8'))

tshark_command = ['tshark','-i','wlp2s0','-T','json','-l']
tshark_process = subprocess.Popen(tshark_command,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
buffer =''

try:
    for line in tshark_process.stdout:
        # print(line.decode('utf-8'))
        # print(len(line.decode('utf-8')))
        try:
            data = line.decode('utf-8')
            if data.startswith('['):
                print(data)
                pass
            elif data.startswith('  },'):
            # elif len(data)==5:
                buffer = buffer + '}'
                print(json.loads(buffer))
                producer.send('test-topic',json.loads(buffer))
                buffer =''
            else:
                buffer = buffer+data
                # print(buffer)
                # print('in else')

            # print(f'The is the buffer {json.loads(buffer)}')
            # producer.send('test-topic',data)
        except json.JSONDecodeError:
            pass
            # print(f"Failed to decode JSON:{line}")
            # print(json.JSONDecodeError)
except KeyboardInterrupt:
    print("Terminating stream")
finally:
    tshark_process.terminate()
    producer.flush()