import osmium
import psycopg2
import psycopg2.extras
import sys
from collections import defaultdict
import os

# --- 설정 (Configuration) ---
DB_NAME = os.environ.get("LS_POI_DB_NAME")
DB_USER = os.environ.get("LS_POI_DB_USER")
DB_PASSWORD = os.environ.get("LS_POI_DB_PASSWORD")
DB_HOST = os.environ.get("LS_POI_DB_HOST")
DB_PORT = os.environ.get("LS_POI_DB_PORT")
OSM_PBF_FILE = "data/sf/sf_city_only.osm.pbf"

if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
    raise RuntimeError("Database environment variables are not all set. Please check your .env file or environment.")

# 어떤 태그를 poi 테이블의 주요 필드로 매핑할지 정의
PRIMARY_FIELDS_MAP = {
    'name': 'name', 'addr:full': 'address', 'phone': 'phone', 
    'website': 'website', 'opening_hours': 'opening_hours'
}

# 어떤 태그를 주 카테고리로 삼을지 우선순위 정의 (첫 번째로 발견되는 것을 사용)
PRIMARY_CATEGORY_KEYS = ['amenity', 'shop', 'tourism', 'leisure', 'historic']

class PoiEtlHandler(osmium.SimpleHandler):
    def __init__(self, db_cursor):
        super().__init__()
        self.cursor = db_cursor
        self.poi_count = 0

    def node(self, n):
        # 이름 태그가 없는 POI는 일단 제외 (중요도가 낮다고 판단)
        if 'name' not in n.tags or not n.location.valid():
            return
        
        tags = {tag.k: tag.v for tag in n.tags}
        
        # 1. 주 카테고리 결정
        primary_cat, primary_val = None, None
        for key in PRIMARY_CATEGORY_KEYS:
            if key in tags:
                primary_cat = key
                primary_val = tags[key]
                break
        
        # 주 카테고리가 없으면 유의미한 POI가 아니라고 판단, 건너뛰기
        if not primary_cat:
            return

        # 2. poi 테이블에 들어갈 데이터 준비
        # 기본 데이터 구조
        poi_main_data = {
                    'name': None, 'address': None, 'phone': None,
                    'website': None, 'opening_hours': None,
                    'rich_description': None,
                    'primary_category': primary_cat,
                    'primary_category_value': primary_val,
                    'osm_id': n.id,
                    'osm_type': 'node',
                    'location_wkt': f'POINT({n.location.lon} {n.location.lat})' # PostGIS WKT 형식
                }



        for osm_key, db_field in PRIMARY_FIELDS_MAP.items():
            if osm_key in tags:
                poi_main_data[db_field] = tags[osm_key]

        poi_main_data['primary_category'] = primary_cat
        poi_main_data['primary_category_value'] = primary_val
        
        # 주소 필드가 비어있다면 addr:* 태그 조합으로 생성
        if 'address' not in poi_main_data:
            addr_parts = [tags.get(k, '') for k in ['addr:housenumber', 'addr:street', 'addr:city', 'addr:postcode']]
            poi_main_data['address'] = ' '.join(filter(None, addr_parts)).strip() or None

        # 3. rich_description 생성 (간단한 버전)
        desc_parts = []
        if 'description' in tags: desc_parts.append(tags['description'])
        if 'cuisine' in tags: desc_parts.append(f"Cuisine: {tags['cuisine']}.")
        if tags.get('wheelchair') == 'yes': desc_parts.append("Wheelchair accessible.")
        poi_main_data['rich_description'] = " ".join(desc_parts) or None
        
        # 4. 데이터베이스에 삽입
        try:
            # 4.1. poi 테이블에 INSERT하고 새로 생성된 id를 반환받음
            insert_poi_query = """
            INSERT INTO poi (osm_id, osm_type, name, location, primary_category, primary_category_value, rich_description, address, phone, website, opening_hours)
            VALUES (%(osm_id)s, %(osm_type)s, %(name)s, ST_SetSRID(ST_GeomFromText(%(location_wkt)s), 4326), 
                    %(primary_category)s, %(primary_category_value)s, %(rich_description)s, %(address)s, 
                    %(phone)s, %(website)s, %(opening_hours)s)
            RETURNING id;
            """
            self.cursor.execute(insert_poi_query, poi_main_data)
            poi_id = self.cursor.fetchone()[0]

            # 4.2. poi_attributes 테이블에 나머지 모든 태그를 INSERT
            attributes_data = [{'poi_id': poi_id, 'key': k, 'value': v} for k, v in tags.items()]
            
            if attributes_data:
                insert_attrs_query = "INSERT INTO poi_attributes (poi_id, key, value) VALUES (%(poi_id)s, %(key)s, %(value)s);"
                psycopg2.extras.execute_batch(self.cursor, insert_attrs_query, attributes_data)

            self.poi_count += 1
            if self.poi_count % 200 == 0:
                print(f"Processed {self.poi_count} POIs...")

        except Exception as e:
            # 에러 발생시 해당 POI는 건너뛰고 로그만 남김
            print(f"Error processing OSM ID {n.id}: {e}")
            # 트랜잭션 전체를 롤백해야 하므로, 여기서 conn.rollback()이 호출되어야 함 (메인 로직에서 처리)

# --- 메인 실행 로직 ---
if __name__ == '__main__':
    conn = None
    try:
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        
        with conn.cursor() as cur:
            # 기존 데이터 클리어 (옵션, 개발 중에는 매번 실행하는게 편함)
            print("Truncating existing data...")
            cur.execute("TRUNCATE TABLE poi, poi_attributes RESTART IDENTITY;")
            
            print("Starting OSM data processing and insertion...")
            # 핸들러에 커서 객체를 전달
            handler = PoiEtlHandler(cur)
            handler.apply_file(OSM_PBF_FILE, locations=True)

        # 모든 것이 성공적이면 트랜잭션 커밋
        conn.commit()
        print(f"\nSuccessfully inserted {handler.poi_count} POIs into the database.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        if conn:
            # 오류 발생 시 모든 변경사항을 롤백
            conn.rollback()
            print("Transaction rolled back.")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")