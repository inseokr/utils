#!/usr/bin/env python3
"""
SF OSM 데이터에서 여행 관련 태그들을 분석하는 스크립트
"""

import subprocess
import json
import sys
from collections import defaultdict, Counter

def run_osmium_command(command):
    """osmium 명령어를 실행하고 결과를 반환"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error running command: {command}")
            print(f"Error: {result.stderr}")
            return None
        return result.stdout
    except Exception as e:
        print(f"Exception running command: {command}")
        print(f"Exception: {e}")
        return None

def analyze_tag_values(osm_file, tag_key):
    """특정 태그의 값들을 분석"""
    print(f"\n=== {tag_key.upper()} 태그 분석 ===")
    
    # osmium을 사용해서 특정 태그를 가진 객체들을 geojson으로 추출
    command = f'osmium export {osm_file} --output-format=geojson'
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return
        
        # GeoJSON 파싱하여 태그 값들을 수집
        tag_values = Counter()
        
        try:
            # 전체 GeoJSON을 파싱
            geojson_data = json.loads(result.stdout)
            
            # FeatureCollection인 경우
            if geojson_data.get('type') == 'FeatureCollection':
                features = geojson_data.get('features', [])
                for feature in features:
                    if 'properties' in feature and tag_key in feature['properties']:
                        value = feature['properties'][tag_key]
                        if value:  # 빈 값이 아닌 경우만
                            tag_values[value] += 1
            # 단일 Feature인 경우
            elif geojson_data.get('type') == 'Feature':
                if 'properties' in geojson_data and tag_key in geojson_data['properties']:
                    value = geojson_data['properties'][tag_key]
                    if value:
                        tag_values[value] += 1
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            return
        
        # 결과 출력
        if tag_values:
            print(f"Total {tag_key} objects: {sum(tag_values.values())}")
            print(f"Unique {tag_key} values: {len(tag_values)}")
            print("\nTop values:")
            for value, count in tag_values.most_common(20):
                print(f"  {value}: {count}")
        else:
            print(f"No {tag_key} values found")
            
    except Exception as e:
        print(f"Exception analyzing {tag_key}: {e}")

def analyze_travel_related_tags():
    """여행 관련 태그들을 분석"""
    osm_file = "data/sf/sf_city_only.osm.pbf"
    
    # 여행 관련 주요 태그들
    travel_tags = [
        'tourism',
        'amenity', 
        'shop',
        'leisure',
        'historic',
        'attraction'
    ]
    
    print("SF OSM 데이터 - 여행 관련 태그 분석")
    print("=" * 50)
    
    for tag in travel_tags:
        analyze_tag_values(osm_file, tag)
        print()  # 빈 줄 추가

def main():
    analyze_travel_related_tags()

if __name__ == "__main__":
    main()
