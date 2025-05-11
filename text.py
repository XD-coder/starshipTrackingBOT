import socketio

sio = socketio.Client()

@sio.event
def connect():
    print('connection established')

@sio.on
def response(data):
    print('message received with ', data)

@sio.event
def disconnect():
    print('disconnected from server')

sio.connect('wss://starbase.nerdpg.live')
sio.emit('message', {'some': 'data'})
sio.wait()