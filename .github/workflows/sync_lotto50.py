import os, time, requests

SB_URL = os.environ['SUPABASE_URL']
SB_KEY = os.environ['SUPABASE_SERVICE_KEY']

HEADERS = {
    'apikey': SB_KEY,
    'Authorization': f'Bearer {SB_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=ignore-duplicates,return=minimal'
}

def fetch_draw(draw_no):
    url = f'https://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo={draw_no}'
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=10)
            d = r.json()
            if d.get('returnValue') == 'success':
                return d
        except Exception as e:
            print(f'  {draw_no}회 시도 {attempt+1} 실패: {e}')
            time.sleep(2)
    return None

def parse(d):
    nums = sorted([d[f'drwtNo{i}'] for i in range(1, 7)])
    n1,n2,n3,n4,n5,n6 = nums
    bonus = d['bnusNo']
    odd = sum(1 for n in nums if n % 2 != 0)
    low = sum(1 for n in nums if n <= 22)
    mx = 1; c = 1
    for i in range(1, len(nums)):
        if nums[i]-nums[i-1] == 1: c += 1; mx = max(mx, c)
        else: c = 1
    return {
        'draw_no': d['drwNo'],
        'draw_date': d['drwNoDate'],
        'num1': n1, 'num2': n2, 'num3': n3,
        'num4': n4, 'num5': n5, 'num6': n6,
        'bonus': bonus,
        'total_sum': sum(nums),
        'odd_count': odd,
        'low_count': low,
        'consec_max': mx,
        'nums_sorted': ','.join(f'{n:02d}' for n in nums)
    }

def save(row):
    r = requests.post(
        f'{SB_URL}/rest/v1/lotto50_draws',
        headers=HEADERS,
        json=row
    )
    return r.status_code in [200, 201]

def get_db_latest():
    r = requests.get(
        f'{SB_URL}/rest/v1/lotto50_draws?select=draw_no&order=draw_no.desc&limit=1',
        headers=HEADERS
    )
    rows = r.json()
    return rows[0]['draw_no'] if rows else 0

def main():
    mode = os.environ.get('SYNC_MODE', 'incremental')
    print(f'=== Lotto50 동기화 시작 (mode={mode}) ===')

    db_latest = get_db_latest()
    print(f'DB 최신 회차: {db_latest}')

    start = 1 if mode == 'full' else db_latest + 1

    new_count = 0
    fail = 0
    draw_no = start

    while fail < 5:
        print(f'  {draw_no}회 수집 중...')
        d = fetch_draw(draw_no)
        if not d:
            fail += 1
            print(f'  → 실패 ({fail}/5)')
            draw_no += 1
            if fail >= 5:
                break
            continue

        fail = 0
        row = parse(d)
        ok = save(row)
        if ok:
            new_count += 1
            print(f'  → {draw_no}회 저장 ({row["draw_date"]})')
        else:
            print(f'  → {draw_no}회 이미 존재')

        draw_no += 1
        time.sleep(0.3)

    print(f'=== 완료! 신규 {new_count}회차 추가 ===')

if __name__ == '__main__':
    main()
