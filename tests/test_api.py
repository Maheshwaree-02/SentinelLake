import uvicorn
from multiprocessing import Process
import time
import requests
import sys

def run_server():
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'serving'))
    from src.serving.main import app
    uvicorn.run(app, host='127.0.0.1', port=8001, log_level='error')

if __name__ == '__main__':
    p = Process(target=run_server)
    p.start()
    print("Waiting for server to start...")
    time.sleep(5)
    
    try:
        print('--- Test 1: No API Key (Should be 401) ---')
        r = requests.get('http://127.0.0.1:8001/recommendations/high-risk')
        print(f'Status: {r.status_code}, Body: {r.json()}')
        
        print('\n--- Test 2: Valid API Key (Should be 200) ---')
        headers = {'X-API-Key': 'efip-dev-key-123'}
        r = requests.get('http://127.0.0.1:8001/recommendations/high-risk?limit=2', headers=headers)
        print(f'Status: {r.status_code}')
        
        print('\n--- Test 3: Caching Speed ---')
        t0 = time.time()
        r = requests.get('http://127.0.0.1:8001/recommendations/high-risk?limit=100', headers=headers)
        t_first = time.time() - t0
        print(f'First request took: {t_first:.4f}s')
        
        t0 = time.time()
        r = requests.get('http://127.0.0.1:8001/recommendations/high-risk?limit=100', headers=headers)
        t_second = time.time() - t0
        print(f'Second (cached) request took: {t_second:.4f}s')
        
        print('\n--- Test 4: POST Disposition ---')
        payload = {
            'transaction_id': 'txn_999',
            'customer_id': 'C123',
            'action_taken': 'BLOCKED',
            'analyst_notes': 'Confirmed via phone',
            'is_fraud': True
        }
        r = requests.post('http://127.0.0.1:8001/disposition', json=payload, headers=headers)
        print(f'Status: {r.status_code}, Body: {r.json()}')
    finally:
        p.terminate()
