import pandas as pd
import requests
import time
import datetime
from pathlib import Path

class GbizInfoFetcher:
    """
    gBizINFO APIから法人情報を取得し、データフレームとして返却するクラス。

    Attributes:
        api_token (str): gBizINFO APIを利用するための認証トークン。
        BASE_URL (str): gBizINFO APIのエンドポイントURL。
    """
    BASE_URL = "https://info.gbiz.go.jp/hojin/v1/hojin/"

    def __init__(self, api_token: str):
        """
        クラスのインスタンスを初期化します。

        Args:
            api_token (str): gBizINFOのAPIトークン。
                               gBizINFOのWebサイトから取得してください。
        """
        if not api_token:
            raise ValueError("APIトークンが指定されていません。")
        self.api_token = api_token

    def _get_value_safely(self, data: dict, keys: list):
        """
        ネストされた辞書から安全に値を取得するヘルパーメソッド。
        キーが存在しない場合はNoneを返す。
        """
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data

    def fetch_data(self, corporate_numbers: list) -> pd.DataFrame:
        """
        指定された法人番号のリストから情報を取得し、一つのデータフレームにまとめます。

        Args:
            corporate_numbers (list): 情報を取得したい法人番号（13桁の文字列）のリスト。

        Returns:
            pd.DataFrame: 取得した法人情報を含むデータフレーム。
                          カラム: ['法人番号', '所在地', '資本金', '設立日', 'Webサイト', '企業概要']
        """
        results = []
        headers = {
            'X-hojinInfo-api-token': self.api_token
        }

        print(f"計 {len(corporate_numbers)} 件の法人情報を取得します...")

        for number in corporate_numbers:
            url = f"{self.BASE_URL}{number}"
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # 200番台以外のステータスコードで例外を発生させる

                data = response.json()
                hojin_info = data.get('hojin-infos', [{}])[0]
                business_summary = hojin_info.get('business_summary', {})

                # 各情報を安全に抽出
                info = {
                    '法人番号': number,
                    '法人名': hojin_info.get('name'),
                    '所在地': hojin_info.get('location'),
                    '従業員数': hojin_info.get('employee_number'),
                    '資本金': self._get_value_safely(hojin_info, ['capital_stock_summary', 'capital_stock']),
                    '設立日': hojin_info.get('date_of_establishment'),
                    'Webサイト': hojin_info.get('company_url'),
                    '企業概要': hojin_info.get('business_summary')
                }
                results.append(info)
                print(f"  - 成功: {number}")

            except requests.exceptions.RequestException as e:
                print(f"  - エラー: {number} の情報取得に失敗しました。詳細: {e}")

            # APIへの負荷軽減のため、リクエストごとに短い待機時間を設ける
            time.sleep(0.5)

        if not results:
            print("有効なデータを1件も取得できませんでした。")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        # カラムの順序を整える
        df = df[['法人番号', '法人名','所在地', '従業員数', '資本金', '設立日', 'Webサイト', '企業概要']]
        print("すべての処理が完了しました。")
        return df

if __name__ == '__main__':
    # 1. gBizINFOで取得したご自身のAPIトークンを入力
    MY_API_TOKEN = "xxxx"
    DATA_DIR = Path("data")

    # 2. 情報を取得したい法人番号のリストを準備
    # (例: グーグル合同会社, 任天堂株式会社)
    target_corporate_numbers = [
        "7010401001556",
        "7030005011875",
        "7011001029649",
    ]

    # 3. クラスをインスタンス化し、メソッドを呼び出します
    fetcher = GbizInfoFetcher(api_token=MY_API_TOKEN)
    df_result = fetcher.fetch_data(corporate_numbers=target_corporate_numbers)

    # 4. 結果（データフレーム）を表示します
    if not df_result.empty:
        print("\n--- 取得結果 ---")
        print(df_result)
        
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        df_result.to_excel(DATA_DIR/"result"/f"gbiz_all_result_{today_str}.xlsx", index=False)