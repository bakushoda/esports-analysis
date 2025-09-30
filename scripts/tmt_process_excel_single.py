import pandas as pd
import numpy as np
from datetime import datetime
import os

class TMTDataIntegratorHighschool:
    def __init__(self, data_dir="data"):
        """
        高校生向けTMT（Trail Making Test）データをdata_masterに統合するクラス
        
        Args:
            data_dir (str): データディレクトリのパス
        """
        self.data_dir = data_dir
        self.master_file = os.path.join(data_dir, "data_master.xlsx")
        self.tmt_file = os.path.join(data_dir, "cognitive", "tiger_2024yobijikken_trailmakingtest(ja)_summary_2509300747.xlsx")
        
    def load_data(self):
        """
        必要なデータファイルを読み込む
        """
        try:
            # data_masterの各シートを読み込み
            print("Loading data_master.xlsx...")
            self.master_data = pd.read_excel(self.master_file, sheet_name="master")
            self.student_list = pd.read_excel(self.master_file, sheet_name="student_list")
            
            # TMTデータを読み込み
            print("Loading TMT data...")
            self.tmt_data = pd.read_excel(self.tmt_file, sheet_name="Inquisit Data")
            
            print(f"Master data: {len(self.master_data)} rows")
            print(f"Student list: {len(self.student_list)} rows")
            print(f"TMT data: {len(self.tmt_data)} rows")
            
            return True
            
        except Exception as e:
            print(f"データ読み込みエラー: {e}")
            return False
    
    def preprocess_tmt_data(self):
        """
        TMTデータの前処理
        """
        print("Preprocessing TMT data...")
        
        # 必要な列のみを抽出
        tmt_columns = ['subjectId', 'combined_errors', 'combined_trailtime']
        self.tmt_processed = self.tmt_data[tmt_columns].copy()
        
        # 欠損値や無効なデータをフィルタリング
        # completed = 1のデータのみを使用（完了したテストのみ）
        if 'completed' in self.tmt_data.columns:
            completed_mask = self.tmt_data['completed'] == 1
            self.tmt_processed = self.tmt_processed[completed_mask].copy()
        
        # 同じ人が複数回テストした場合、最初のレコードのみを使用
        print("Handling duplicate tests...")
        original_count = len(self.tmt_processed)
        
        # subjectIdでソート
        self.tmt_processed = self.tmt_processed.sort_values(['subjectId'])
        
        # 同じ人の重複を除去（最初のレコードを保持）
        self.tmt_processed = self.tmt_processed.drop_duplicates(
            subset=['subjectId'], 
            keep='first'
        ).reset_index(drop=True)
        
        duplicates_removed = original_count - len(self.tmt_processed)
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate tests (same person)")
        
        print(f"Processed TMT data: {len(self.tmt_processed)} rows")
        
        # データの確認
        print("\nTMT data sample:")
        print(self.tmt_processed.head())
        print(f"\nUnique subjects: {self.tmt_processed['subjectId'].nunique()}")
    
    def match_data_by_participant_id(self):
        """
        subjectIdとparticipant_idでデータをマッチング（一回きりの実験）
        """
        print("Matching data by subjectId and participant_id...")
        
        # マッチング用のデータフレームを準備
        matches = []
        unmatched_tmt = []
        
        for _, tmt_row in self.tmt_processed.iterrows():
            subject_id = tmt_row['subjectId']
            
            # subjectIdから番号を抽出
            import re
            if pd.isna(subject_id) or (isinstance(subject_id, str) and subject_id.strip() == ''):
                continue
                
            # subjectIdから接頭辞と数字を抽出（任意の接頭辞 + 数字の形式）
            subject_str = str(subject_id).lower()
            prefix_match = re.match(r'^([a-z]+)(\d+)', subject_str)
            if not prefix_match:
                unmatched_tmt.append({
                    'subject_id': subject_id,
                    'reason': 'Invalid subjectId format (should be prefix + number)'
                })
                continue
            
            # 接頭辞と数字を抽出
            prefix = prefix_match.group(1)  # 任意の接頭辞
            number = prefix_match.group(2)  # 数字部分
            
            # 接頭辞 + 番号の形式でparticipant_idを作成
            potential_participant_id = f"{prefix}{number}"
            
            # master_dataでparticipant_idを検索
            master_subset = self.master_data[self.master_data['participant_id'] == potential_participant_id].copy()
            
            if len(master_subset) == 0:
                unmatched_tmt.append({
                    'subject_id': subject_id,
                    'reason': f'Participant ID {potential_participant_id} not found in master data'
                })
                continue
            
            # 一回きりの実験なので、最初のレコードにマッチング
            best_match = master_subset.iloc[0]  # 最初のレコードを取得
            
            match_info = {
                'master_index': best_match.name,
                'participant_id': potential_participant_id,
                'subject_id': subject_id,
                'combined_errors': tmt_row['combined_errors'],
                'combined_trailtime': tmt_row['combined_trailtime']
            }
            matches.append(match_info)
        
        self.matches_df = pd.DataFrame(matches)
        self.unmatched_df = pd.DataFrame(unmatched_tmt)
        
        print(f"Found {len(self.matches_df)} matches")
        print(f"Unmatched TMT records: {len(self.unmatched_df)}")
        
        if len(self.matches_df) > 0:
            print("\nMatching summary:")
            print(f"Sample matches:")
            print(self.matches_df[['subject_id', 'participant_id']].head(10))
        
        if len(self.unmatched_df) > 0:
            print(f"\nUnmatched records sample:")
            print(self.unmatched_df.head())
            
            # 未マッチの理由別統計
            reason_stats = self.unmatched_df['reason'].value_counts()
            print(f"\nUnmatched reasons:")
            for reason, count in reason_stats.items():
                print(f"  {reason}: {count}")
    
    def update_master_data(self):
        """
        マッチしたデータでmasterデータを更新
        """
        if len(self.matches_df) == 0:
            print("No matches found. Nothing to update.")
            return
        
        print("Updating master data...")
        
        # masterデータのコピーを作成
        updated_master = self.master_data.copy()
        
        # マッチしたデータで更新
        for _, match in self.matches_df.iterrows():
            idx = match['master_index']
            
            # TMTデータで更新
            updated_master.loc[idx, 'tmt_combined_errors'] = match['combined_errors']
            updated_master.loc[idx, 'tmt_combined_trailtime'] = match['combined_trailtime']
        
        self.updated_master = updated_master
        
        # 更新された行数をカウント
        updated_rows = len(self.matches_df)
        print(f"Updated {updated_rows} rows in master data")
        
        # 更新統計
        print("\nUpdate statistics:")
        print(f"tmt_combined_errors updated: {self.updated_master['tmt_combined_errors'].notna().sum()}")
        print(f"tmt_combined_trailtime updated: {self.updated_master['tmt_combined_trailtime'].notna().sum()}")
    
    def save_updated_data(self):
        """
        更新されたデータを保存（自動的にバックアップも作成）
        """
        # backupディレクトリを作成
        backup_dir = os.path.join(self.data_dir, "backup")
        os.makedirs(backup_dir, exist_ok=True)
        
        # バックアップを作成
        backup_file = os.path.join(backup_dir, f"data_master_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
        print(f"Creating backup: {backup_file}")
        
        # 元のデータをバックアップ
        with pd.ExcelWriter(backup_file, engine='openpyxl') as writer:
            # 元のmaster_dataを読み込んでバックアップ
            original_master = pd.read_excel(self.master_file, sheet_name="master")
            original_student_list = pd.read_excel(self.master_file, sheet_name="student_list")
            original_school_list = pd.read_excel(self.master_file, sheet_name="school_list")
            
            original_master.to_excel(writer, sheet_name='master', index=False)
            original_student_list.to_excel(writer, sheet_name='student_list', index=False)
            original_school_list.to_excel(writer, sheet_name='school_list', index=False)
        
        # 更新されたデータを保存
        if hasattr(self, 'updated_master') and self.updated_master is not None:
            print(f"Saving updated data to {self.master_file}")
            with pd.ExcelWriter(self.master_file, engine='openpyxl') as writer:
                self.updated_master.to_excel(writer, sheet_name='master', index=False)
                original_student_list.to_excel(writer, sheet_name='student_list', index=False)
                original_school_list.to_excel(writer, sheet_name='school_list', index=False)
        else:
            print("No data to update - keeping original data_master.xlsx")
        
        print("TMT data integration completed successfully!")
    
    def generate_report(self):
        """
        統合結果のレポートを生成
        """
        if not hasattr(self, 'matches_df') or len(self.matches_df) == 0:
            print("No data to report.")
            return
        
        print("\n" + "="*50)
        print("TMT DATA INTEGRATION REPORT (HIGHSCHOOL)")
        print("="*50)
        
        print(f"Source file: {os.path.basename(self.tmt_file)}")
        print(f"Target file: {os.path.basename(self.master_file)}")
        
        print(f"\nTotal TMT records processed: {len(self.tmt_processed)}")
        print(f"Successfully matched records: {len(self.matches_df)}")
        print(f"Match rate: {len(self.matches_df)/len(self.tmt_processed)*100:.1f}%")
        
        # 参加者別の統計
        participant_stats = self.matches_df.groupby('participant_id').size()
        print(f"\nParticipants updated: {len(participant_stats)}")
        print(f"Records per participant: {participant_stats.mean():.1f} (avg)")
    
    def run_integration(self):
        """
        データ統合プロセス全体を実行
        """
        print("Starting TMT data integration process (Highschool)...")
        print("="*50)
        
        # 1. データ読み込み
        if not self.load_data():
            return False
        
        # 2. TMTデータの前処理
        self.preprocess_tmt_data()
        
        # 3. データマッチング
        self.match_data_by_participant_id()
        
        # 4. masterデータの更新
        self.update_master_data()
        
        # 5. レポート生成
        self.generate_report()
        
        # 6. データ保存（自動的にバックアップも作成）
        self.save_updated_data()
        
        return True

# 使用例
if __name__ == "__main__":
    # esports-analysis/dataディレクトリで実行
    integrator = TMTDataIntegratorHighschool("data")
    integrator.run_integration()
