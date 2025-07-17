import os
import sys
from collections import defaultdict
import psycopg2
import psycopg2.extras
import osmium
from dotenv import load_dotenv
# from sentence_transformers import SentenceTransformer

# --- 0. 설정 및 초기화 ---

# .env 파일에서 환경 변수 로드
load_dotenv()

# 데이터베이스 접속 정보
DB_NAME = os.getenv("LS_POI_DB_NAME")
DB_USER = os.getenv("LS_POI_DB_USER")
DB_PASSWORD = os.getenv("LS_POI_DB_PASSWORD")
DB_HOST = os.getenv("LS_POI_DB_HOST")
DB_PORT = os.getenv("LS_POI_DB_PORT")
OSM_PBF_FILE = "data/raw/usa/california/sf_city_only.osm.pbf"

if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
    raise RuntimeError("DB 환경 변수가 모두 설정되지 않았습니다. .env 파일을 확인해주세요.")

# 텍스트 임베딩 모델 로드 (초기 로딩에 시간이 걸릴 수 있습니다) [현재 주석 처리]
# print("Loading sentence-transformer model...") 
# embedding_model = SentenceTransformer('all-MiniLM-L6-v2') # 384차원 모델 사용
# print("Model loaded.")

# OSM 태그를 Google 카테고리로 매핑하는 규칙 (역방향 매핑)
def get_osm_to_google_mapping():
    google_categories = {
        # === 숙박 (Lodging) ===
        'lodging': ['hotel', 'motel', 'hostel', 'guest_house', 'apartment', 'camp_site', 'caravan_site'],
        'accommodation': ['hotel', 'motel', 'hostel', 'guest_house'],
        # === 음식 및 음료 (Food & Drink) ===
        'restaurant': ['restaurant', 'fast_food', 'cafe', 'bar', 'pub', 'biergarten', 'food_court'],
        'food': ['restaurant', 'fast_food', 'cafe', 'bar', 'pub', 'bakery', 'deli', 'ice_cream', 'butcher', 'seafood'],
        'meal_takeaway': ['fast_food'],
        'cafe': ['cafe', 'coffee'],
        'bar': ['bar', 'pub', 'nightclub', 'stripclub'],
        'night_club': ['nightclub', 'stripclub'],
        # === 관광 명소 (Tourist Attraction) ===
        'tourist_attraction': ['attraction', 'museum', 'gallery', 'monument', 'memorial', 'lighthouse', 'fort', 'ruins', 'landmark', 'historic', 'ship', 'artwork', 'viewpoint', 'theme_park'],
        'museum': ['museum', 'gallery'],
        'zoo': ['zoo', 'aquarium', 'animal'],
        'amusement_park': ['theme_park', 'amusement_arcade'],
        'park': ['park', 'garden', 'playground', 'dog_park', 'common', 'nature_reserve'],
        'place_of_worship': ['place_of_worship', 'church', 'monastery'],
        'point_of_interest': ['viewpoint', 'information', 'artwork', 'picnic_site', 'fountain', 'monument', 'memorial'],
        # === 쇼핑 (Shopping) ===
        'shopping_mall': ['mall', 'department_store'],
        'store': [
            'clothes', 'books', 'jewelry', 'furniture', 'gift', 'florist', 'alcohol', 'wine',
            'tobacco', 'cannabis', 'toys', 'pet', 'shoes', 'art', 'tattoo', 'antiques', 'craft',
            'electronics', 'music', 'boutique', 'computer', 'charity', 'second_hand', 'hifi', 'photo',
            'video_games', 'general', 'convenience', 'supermarket'],
        'convenience_store': ['convenience'],
        'supermarket': ['supermarket', 'greengrocer'],
        'clothing_store': ['clothes', 'fashion_accessories', 'tailor'],
        'book_store': ['books'],
        'jewelry_store': ['jewelry', 'watches'],
        'gift_shop': ['gift'],
        'hardware_store': ['hardware', 'doityourself'],
        'home_goods_store': ['furniture', 'interior_decoration', 'carpet', 'lighting', 'kitchen'],
        # === 레크리에이션 및 엔터테인먼트 (Recreation & Entertainment) ===
        'gym': ['fitness_centre', 'sports_centre', 'fitness_station', 'dojo'],
        'spa': ['spa', 'massage', 'sauna', 'tanning_salon'],
        'beauty_salon': ['beauty', 'hairdresser', 'barber', 'pet_grooming'],
        'swimming_pool': ['swimming_pool', 'swimming_area'],
        'golf_course': ['golf_course', 'miniature_golf'],
        'bowling_alley': ['bowling_alley'],
        'movie_theater': ['theatre', 'cinema'],
        'library': ['library', 'public_bookcase'],
        'stadium': ['stadium', 'pitch', 'track', 'sports_hall'],
        # === 교통 (Transportation) ===
        'airport': [],
        'bus_station': ['bus_station'],
        'parking': ['parking', 'parking_space', 'parking_entrance', 'motorcycle_parking'],
        'subway_station': ['subway_station'],
        'taxi_stand': ['taxi'],
        'train_station': [],
        'transit_station': ['station', 'ferry_terminal', 'bus_station'],
        # === 서비스 (Services) ===
        'bank': ['bank', 'atm', 'credit_union'],
        'hospital': ['hospital'],
        'pharmacy': ['pharmacy', 'chemist'],
        'post_office': ['post_office', 'post_box', 'post_depot'],
        'gas_station': ['fuel'],
        'car_rental': ['car_rental'],
        'car_repair': ['car_repair', 'car_wash', 'tyres'],
        'bicycle_store': ['bicycle_rental', 'bicycle', 'bicycle_repair_station'],
        'laundry': ['laundry', 'dry_cleaning'],
        'locksmith': ['locksmith'],
        'moving_company': ['moving_storage', 'storage_rental'],
        'travel_agency': ['travel_agency'],
        # === 교육 (Education) ===
        'school': ['school', 'kindergarten', 'college', 'university', 'prep_school', 'music_school', 'language_school', 'dancing_school'],
        # === 건강 및 의료 (Health & Medical) ===
        'doctor': ['doctors', 'clinic'],
        'dentist': ['dentist'],
        'physiotherapist': [],
        'veterinary_care': ['veterinary'],
        # === 기타 (Miscellaneous) ===
        'establishment': ['yes'],
        'police': ['police'],
        'fire_station': ['fire_station'],
        'courthouse': ['courthouse'],
        'embassy': [],
        'town_hall': ['townhall', 'community_centre'],
    }
    # 역매핑: OSM value -> Google 카테고리 리스트
    osm_to_google = {}
    for google_cat, osm_values in google_categories.items():
        for osm_value in osm_values:
            if osm_value not in osm_to_google:
                osm_to_google[osm_value] = []
            osm_to_google[osm_value].append(google_cat)
    return osm_to_google

# POI로 저장하지 않을 태그들 (필터링용)
IGNORED_TAGS = {
    'amenity': ['bench', 'waste_basket', 'bicycle_parking', 'vending_machine', 'toilets', 'drinking_water', 'shelter', 'recycling'],
    'leisure': ['picnic_table', 'outdoor_seating', 'bleachers']
}

class PoiEtlHandler(osmium.SimpleHandler):
    def __init__(self, db_cursor):
        super().__init__()
        self.cursor = db_cursor
        self.poi_count = 0
        self.mapping_rules = get_osm_to_google_mapping()

    def create_text_for_embedding(self, tags, google_cats):
        """임베딩을 생성할 대표 텍스트를 만듭니다."""
        name = tags.get('name', '')
        # 카테고리들을 텍스트로 변환 (중복 제거)
        categories_text = ', '.join(sorted(list(set(google_cats))))
        
        # 주소나 설명 추가
        address = tags.get('addr:street', '')
        description = tags.get('description', '')
        
        # 최종 텍스트 조합
        full_text = f"장소: {name}. 카테고리: {categories_text}. 주소: {address}. 설명: {description}."
        return full_text

    def node(self, n):
        # 이름이 없거나 위치가 없으면 건너뛰기
        if 'name' not in n.tags or not n.location.valid():
            return
        
        tags = {tag.k: tag.v for tag in n.tags}
        
        # --- 1. 불필요한 POI 필터링 ---
        for key, values_to_ignore in IGNORED_TAGS.items():
            if key in tags and tags[key] in values_to_ignore:
                return # 이 POI는 저장하지 않고 건너뜀

        # --- 2. 데이터 추출 및 가공 ---
        osm_id = n.id
        osm_type = 'node'
        name = tags.get('name')
        location = f'POINT({n.location.lon} {n.location.lat})'
        addr_parts = [tags.get(k, '') for k in ['addr:housenumber', 'addr:street', 'addr:city']]
        address = ' '.join(filter(None, addr_parts)).strip() or None

        # Google 카테고리 매핑
        google_categories = set()
        for key, value in tags.items():
            # 복합 태그 처리 (예: amenity=foo;bar)
            split_values = [v.strip() for v in value.split(';')]
            for v_part in split_values:
                if v_part in self.mapping_rules:
                    google_categories.update(self.mapping_rules[v_part])
        google_categories_list = sorted(list(google_categories)) if google_categories else None

        # --- 3. 임베딩 생성 ---
        # text_to_embed = self.create_text_for_embedding(tags, google_categories)
        # text_embedding = embedding_model.encode(text_to_embed).tolist()
        
        # --- 4. 데이터베이스에 삽입 ---
        try:
            # 4.1. poi 테이블에 INSERT
            poi_data = {
                'name': name, 'location': location, 'address': address,
                'google_categories': google_categories_list, 'osm_id': osm_id, 'osm_type': osm_type,
                'region': 'San Francisco', 'summary_description': tags.get('description', '')
            }
            insert_poi_query = """
                INSERT INTO poi (name, location, address, google_categories, osm_id, osm_type, region, summary_description)
                VALUES (%(name)s, ST_SetSRID(ST_GeomFromText(%(location)s), 4326), 
                        %(address)s, %(google_categories)s, %(osm_id)s, %(osm_type)s, %(region)s, %(summary_description)s)
                RETURNING id;
            """
            self.cursor.execute(insert_poi_query, poi_data)
            poi_id = self.cursor.fetchone()[0]

            # 4.2. poi_attributes 테이블에 INSERT
            attributes_data = [{'poi_id': poi_id, 'key': k, 'value': v} for k, v in tags.items()]
            if attributes_data:
                insert_attrs_query = "INSERT INTO poi_attributes (poi_id, key, value) VALUES (%(poi_id)s, %(key)s, %(value)s);"
                psycopg2.extras.execute_batch(self.cursor, insert_attrs_query, attributes_data)
            
            # 4.3. poi_embeddings 테이블에 INSERT
            # embedding_data = {'poi_id': poi_id, 'text_embedding': text_embedding}
            # insert_embed_query = "INSERT INTO poi_embeddings (poi_id, text_embedding) VALUES (%(poi_id)s, %(text_embedding)s);"
            # self.cursor.execute(insert_embed_query, embedding_data)

            self.poi_count += 1
            if self.poi_count % 100 == 0:
                print(f"Processed {self.poi_count} POIs...")

        except Exception as e:
            print(f"Error processing OSM ID {n.id}: {e}")
            self.cursor.connection.rollback() # 오류 발생 시 현재 트랜잭션의 이 POI 관련 작업만 롤백

if __name__ == '__main__':
    conn = None
    try:
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        
        with conn.cursor() as cur:
            print("Truncating existing data...")
            cur.execute("TRUNCATE TABLE poi, poi_attributes, poi_semantic_tags RESTART IDENTITY CASCADE;")
            
            print("Starting OSM data processing...")
            handler = PoiEtlHandler(cur)
            handler.apply_file(OSM_PBF_FILE, locations=True)

        conn.commit()
        print(f"\nSuccessfully inserted {handler.poi_count} POIs into the database.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")