# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import os
import datetime
import json
from time import sleep
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from typing import List, Dict, Any
from pathlib import Path
import pandas as pd

GOOGLE_API_KEY          = "xxxx"
CUSTOM_SEARCH_ENGINE_ID = "xxxx"

DATA_DIR = Path("data")

def makeDir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

def build_google_api_service(api_name: str, api_version: str, api_key: str) -> Resource:
    """Google APIのサービスオブジェクトを構築

    Args:
        api_name (str): 利用するAPIの名前 (例: 'customsearch')。
        api_version (str): 利用するAPIのバージョン (例: 'v1')。
        api_key (str): Google APIキー。

    Returns:
        Resource: APIと通信するためのリソースオブジェクト。
    """
    # build関数の引数`version`に、引数で受け取った`api_version`を正しく渡すように修正。
    service = build(api_name, api_version, developerKey=api_key)
    return service
    
def get_google_search_results(
    service: Resource,
    keyword: str,
    custom_search_engine_id: str,
    page_limit: int = 1,
    num_of_results: int = 10,
) -> List[Dict[str, Any]]:
    """Google Custom Search APIを使い、指定されたキーワードの検索結果を取得

    複数ページにまたがる検索結果を、1つのリストにまとめて返す。

    Args:
        service (Resource): `build_google_api_service`で構築されたサービスオブジェクト。
        keyword (str): 検索したいキーワード。
        custom_search_engine_id (str): カスタム検索エンジンID。
        page_limit (int, optional): 取得する検索結果のページ数。デフォルトは1。
        num_of_results (int, optional): 1ページあたりの検索結果数 (最大10)。デフォルトは10。

    Returns:
        List[Dict[str, Any]]: 検索結果のアイテムのリスト。検索結果がない場合は空のリストを返す。
                               各アイテムはタイトル、リンク、スニペットなどを含む辞書。
    """
    all_search_results = []
    start_index = 1
    
    # page_limitで指定された回数だけループして、複数ページの結果を取得
    for _ in range(page_limit):
        try:
            # APIへの連続リクエストを避けるための待機
            sleep(1)
            
            response = service.cse().list(
                q=keyword,
                cx=custom_search_engine_id,
                lr='lang_ja',
                num=num_of_results,
                start=start_index
            ).execute()

            # responseから検索アイテム('items')を取得し、リストに追加
            items = response.get('items', [])
            if not items:
                print("これ以上検索結果がありません。")
                break
            
            all_search_results.extend(items)

            # 次のページの情報('nextPage')があれば、次のstart_indexを取得
            next_page = response.get('queries', {}).get('nextPage', [None])[0]
            if next_page:
                start_index = next_page.get('startIndex')
            else:
                # 次のページがなければループを終了
                print("最後のページに到達しました。")
                break

        except HttpError as e:
            print(f"APIリクエスト中にエラーが発生しました: {e}")
            break
        except Exception as e:
            print(f"予期せぬエラーが発生しました: {e}")
            break
            
    return all_search_results
    
def save_results_as_json(
    search_results: List[Dict[str, Any]],
    save_dir: Path,
    filename: str
) -> None:
    """検索結果をタイムスタンプ付きでJSONファイルに保存する。

    指定された保存先ディレクトリが存在しない場合は自動的に作成。

    Args:
        search_results (List[Dict[str, Any]]): 保存する検索結果のリスト。
        save_dir (Path): 結果を保存するファイルのパス。
        filename (str): 保存するファイル名。
    """
    try:
        # 保存先ディレクトリが存在しない場合は作成
        save_dir.mkdir(parents=True, exist_ok=True)

        now = datetime.datetime.now()
        
        # 保存するデータ構造を作成
        output_data = {
            'snapshot_ymd': now.strftime("%Y%m%d"),
            'snapshot_timestamp': now.strftime("%Y/%m/%d %H:%M:%S"),
            'response': search_results
        }
        

        # JSONファイルに書き込み
        file_path = os.path.join(save_dir, filename)
        with open(file_path, mode='w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
        
    except Exception as e:
        print(f"JSONファイルの保存中にエラーが発生しました: {e}")


def preprocess_to_dataframe(
    search_results: List[Dict[str, Any]],
    keys_to_extract: List[str]
) -> pd.DataFrame:
    """検索結果の辞書のリストから指定したキーの値を抽出し、DataFrameに変換する。

    'pagemap.cse_image.0.src' のようにドットで区切ることで、
    入れ子になった辞書やリストの要素も抽出できる。

    Args:
        search_results (List[Dict[str, Any]]): `get_google_search_results`で取得した検索結果のリスト。
        keys_to_extract (List[str]): 抽出したいキーのリスト。

    Returns:
        pd.DataFrame: 抽出したデータから作成されたpandas DataFrame。
    """
    processed_data = []

    for item in search_results:
        extracted_row = {}
        for key_path in keys_to_extract:
            # ドットを基準にキーを分割
            keys = key_path.split('.')
            value = item
            # 入れ子になった辞書をたどる
            for key in keys:
                try:
                    # キーが数値の場合はリストのインデックスとしてアクセス
                    if key.isdigit() and isinstance(value, list):
                        value = value[int(key)]
                    # そうでなければ辞書のキーとしてアクセス
                    elif isinstance(value, dict):
                        value = value.get(key)
                    else:
                        value = None
                        break
                except (KeyError, IndexError, TypeError):
                    value = None
                    break
            extracted_row[key_path] = value
        processed_data.append(extracted_row)
    
    return pd.DataFrame(processed_data)

    
if __name__ == '__main__':
    
    # キーワードリスト
    target_keywords = [
        'ときがわ町商工会 不正',
        '株式会社ジェイアール東日本企画 不正'
    ]
    
    # 全キーワードの結果を格納するデータフレーム
    all_result_df=pd.DataFrame()

    # サービスオブジェクトの構築
    service=build_google_api_service(api_name="customsearch",api_version="v1",api_key=GOOGLE_API_KEY)
    
    # 検索結果の取得
    for keyword in target_keywords:
        all_search_results = get_google_search_results(
            service=service,
            keyword=keyword,
            custom_search_engine_id=CUSTOM_SEARCH_ENGINE_ID,
            page_limit=1,
            num_of_results=10
        )
        
        #Jsonファイルで保存
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        save_results_as_json(
            search_results=all_search_results,
            save_dir=DATA_DIR / "response",
            filename=f"response_{today_str}.json"
        )

        # Jsonから必要な要素を抽出し、データフレームに変換
        result_df=preprocess_to_dataframe(
            search_results=all_search_results,
            keys_to_extract=["title","snippet"]
        )
        
        result_df["keyword"] = keyword
        all_result_df =pd.concat([all_result_df, result_df])
    
    
    all_result_df.to_excel(DATA_DIR/"result"/f"google_all_result_{today_str}.xlsx", index=False)
