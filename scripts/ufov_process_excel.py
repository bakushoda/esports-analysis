import pandas as pd
import numpy as np
from datetime import datetime
import os

class UFOVDataIntegrator:
    def __init__(self, data_dir="data"):
        """
        UFOV（Useful Field of View）データをdata_masterに統合するクラス
        
        Args:
            data_dir (str): データディレクトリのパス
        """
        self.data_dir = data_dir
        self.master_file = os.path.join(data_dir, "data_master.xlsx")
        self.ufov_file = os.path.join(data_dir, "tiger_2024yobijikken_usefulfieldofviewtest(ja)_summary_2507120824.xlsx")
        
    def load_data(self):
        """
        必要なデータファイルを読み込む
        """
        try:
            # data_masterの各シートを読み込み
            print("Loading data_master.xlsx...")
            self.master_data = pd.read_excel(self.master_file, sheet_name="master")
            self.student_list = pd.read_excel(self.master_file, sheet_name="student_list")
            self.school_list = pd.read_excel(self.master_file, sheet_name="school_list")
            
            # UFOVデータを読み込み
            print("Loading UFOV data...")
            self.ufov_data = pd.read_excel(self.ufov_file, sheet_name="Inquisit Data")
            
            print(f"Master data: {len(self.master_data)} rows")
            print(f"Student list: {len(self.student_list)} rows")
            print(f"UFOV data: {len(self.ufov_data)} rows")
            
            return True
            
        except Exception as e:
            print(f"データ読み込みエラー: {e}")
            return False
    
    def preprocess_ufov_data(self):
        """
        UFOVデータの前処理
        """
        print("Preprocessing UFOV data...")
        
        # 日付形式を統一（startDateをdatetime形式に変換）
        self.ufov_data['startDate'] = pd.to_datetime(self.ufov_data['startDate'])
        
        # 必要な列のみを抽出
        ufov_columns = ['subjectId', 'startDate', 'startTime', 'subtest1_threshold', 'subtest2_threshold', 'subtest3_threshold']
        self.ufov_processed = self.ufov_data[ufov_columns].copy()
        
        # 欠損値や無効なデータをフィルタリング
        # completed = 1のデータのみを使用（完了したテストのみ）
        if 'completed' in self.ufov_data.columns:
            completed_mask = self.ufov_data['completed'] == 1
            self.ufov_processed = self.ufov_processed[completed_mask].copy()
        
        # 同じ日に同じ人が複数回テストした場合、最初のレコード（最早の時刻）のみを使用
        print("Handling duplicate tests on same day...")
        original_count = len(self.ufov_processed)
        
        # startTimeも考慮してソート
        self.ufov_processed = self.ufov_processed.sort_values(['subjectId', 'startDate', 'startTime'])
        
        # 同じ人・同じ日の重複を除去（最初のレコードを保持）
        self.ufov_processed = self.ufov_processed.drop_duplicates(
            subset=['subjectId', 'startDate'], 
            keep='first'
        ).reset_index(drop=True)
        
        duplicates_removed = original_count - len(self.ufov_processed)
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate tests (same person, same day)")
        
        print(f"Processed UFOV data: {len(self.ufov_processed)} rows")
        
        # データの確認
        print("\nUFOV data sample:")
        print(self.ufov_processed.head())
        print(f"\nUnique subjects: {self.ufov_processed['subjectId'].nunique()}")
        print(f"Date range: {self.ufov_processed['startDate'].min()} to {self.ufov_processed['startDate'].max()}")
        
        # 同じ人の複数日テスト確認
        subject_date_counts = self.ufov_processed.groupby('subjectId')['startDate'].nunique()
        multiple_sessions = subject_date_counts[subject_date_counts > 1]
        if len(multiple_sessions) > 0:
            print(f"\nParticipants with multiple test sessions: {len(multiple_sessions)}")
            print(f"Max sessions per participant: {multiple_sessions.max()}")
    
    def create_name_mapping(self):
        """
        氏名からparticipant_idへのマッピングを作成
        """
        print("Creating name to participant_id mapping...")
        
        # student_listから氏名とparticipant_idのマッピングを作成
        self.name_to_pid = dict(zip(self.student_list['氏名'], self.student_list['participant_id']))
        
        print(f"Created mapping for {len(self.name_to_pid)} students")
        
        # ufov_dataの氏名がstudent_listに存在するかチェック
        ufov_names = set(self.ufov_processed['subjectId'].unique())
        master_names = set(self.student_list['氏名'].unique())
        
        matched_names = ufov_names & master_names
        unmatched_names = ufov_names - master_names
        
        print(f"Matched names: {len(matched_names)}")
        print(f"Unmatched names: {len(unmatched_names)}")
        
        if unmatched_names:
            print(f"Unmatched names: {list(unmatched_names)}")
    
    def match_data_by_date_and_name(self):
        """
        氏名と測定日でデータをマッチング（複数回測定に対応）
        """
        print("Matching data by name and measurement date...")
        
        # master_dataの日付形式を統一
        self.master_data['measurement_date'] = pd.to_datetime(self.master_data['measurement_date'])
        
        # マッチング用のデータフレームを準備
        matches = []
        unmatched_ufov = []
        
        for _, ufov_row in self.ufov_processed.iterrows():
            name = ufov_row['subjectId']
            test_date = ufov_row['startDate']
            
            # 氏名がstudent_listにあるかチェック
            if name not in self.name_to_pid:
                unmatched_ufov.append({
                    'name': name,
                    'test_date': test_date,
                    'reason': 'Name not found in student_list'
                })
                continue
            
            participant_id = self.name_to_pid[name]
            
            # 同じparticipant_idのレコードを全て取得
            master_subset = self.master_data[self.master_data['participant_id'] == participant_id].copy()
            
            if len(master_subset) == 0:
                unmatched_ufov.append({
                    'name': name,
                    'test_date': test_date,
                    'reason': 'Participant ID not found in master data'
                })
                continue
            
            # 各measurement_waveに対して最適なマッチを探す
            best_match = None
            min_date_diff = float('inf')
            
            for _, master_row in master_subset.iterrows():
                date_diff = abs((master_row['measurement_date'] - test_date).days)
                
                # 60日以内かつ、現在の最適マッチより近い場合
                if date_diff <= 60 and date_diff < min_date_diff:
                    min_date_diff = date_diff
                    best_match = master_row
            
            if best_match is not None:
                match_info = {
                    'master_index': best_match.name,
                    'participant_id': participant_id,
                    'name': name,
                    'measurement_wave': best_match['measurement_wave'],
                    'cohort': best_match['cohort'],
                    'master_date': best_match['measurement_date'],
                    'ufov_date': test_date,
                    'date_diff': min_date_diff,
                    'subtest1_threshold': ufov_row['subtest1_threshold'],
                    'subtest2_threshold': ufov_row['subtest2_threshold'],
                    'subtest3_threshold': ufov_row['subtest3_threshold']
                }
                matches.append(match_info)
            else:
                unmatched_ufov.append({
                    'name': name,
                    'test_date': test_date,
                    'reason': f'No measurement within 15 days (participant_id: {participant_id})'
                })
        
        self.matches_df = pd.DataFrame(matches)
        self.unmatched_df = pd.DataFrame(unmatched_ufov)
        
        print(f"Found {len(self.matches_df)} matches")
        print(f"Unmatched UFOV records: {len(self.unmatched_df)}")
        
        if len(self.matches_df) > 0:
            print("\nMatching summary:")
            print(f"Average date difference: {self.matches_df['date_diff'].mean():.1f} days")
            print(f"Max date difference: {self.matches_df['date_diff'].max()} days")
            
            # 測定波別の統計
            wave_stats = self.matches_df.groupby('measurement_wave').size()
            print(f"\nMatches by measurement wave:")
            for wave, count in wave_stats.items():
                print(f"  Wave {wave}: {count} matches")
            
            print(f"\nSample matches:")
            print(self.matches_df[['name', 'measurement_wave', 'master_date', 'ufov_date', 'date_diff']].head(10))
        
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
            
            # UFOVデータで更新
            updated_master.loc[idx, 'ufov_subtest1_threshold'] = match['subtest1_threshold']
            updated_master.loc[idx, 'ufov_subtest2_threshold'] = match['subtest2_threshold']
            updated_master.loc[idx, 'ufov_subtest3_threshold'] = match['subtest3_threshold']
        
        self.updated_master = updated_master
        
        # 更新された行数をカウント
        updated_rows = len(self.matches_df)
        print(f"Updated {updated_rows} rows in master data")
        
        # 更新統計
        print("\nUpdate statistics:")
        print(f"ufov_subtest1_threshold updated: {self.updated_master['ufov_subtest1_threshold'].notna().sum()}")
        print(f"ufov_subtest2_threshold updated: {self.updated_master['ufov_subtest2_threshold'].notna().sum()}")
        print(f"ufov_subtest3_threshold updated: {self.updated_master['ufov_subtest3_threshold'].notna().sum()}")
    
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
        with pd.ExcelWriter(backup_file, engine='openpyxl') as writer:
            self.master_data.to_excel(writer, sheet_name='master', index=False)
            self.student_list.to_excel(writer, sheet_name='student_list', index=False)
            self.school_list.to_excel(writer, sheet_name='school_list', index=False)
        
        # 更新されたデータを保存
        print(f"Saving updated data to {self.master_file}")
        with pd.ExcelWriter(self.master_file, engine='openpyxl') as writer:
            self.updated_master.to_excel(writer, sheet_name='master', index=False)
            self.student_list.to_excel(writer, sheet_name='student_list', index=False)
            self.school_list.to_excel(writer, sheet_name='school_list', index=False)
        
        print("UFOV data integration completed successfully!")
    
    def generate_report(self):
        """
        統合結果のレポートを生成
        """
        if not hasattr(self, 'matches_df') or len(self.matches_df) == 0:
            print("No data to report.")
            return
        
        print("\n" + "="*50)
        print("UFOV DATA INTEGRATION REPORT")
        print("="*50)
        
        print(f"Source file: {os.path.basename(self.ufov_file)}")
        print(f"Target file: {os.path.basename(self.master_file)}")
        print(f"Integration date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\nTotal UFOV records processed: {len(self.ufov_processed)}")
        print(f"Successfully matched records: {len(self.matches_df)}")
        print(f"Match rate: {len(self.matches_df)/len(self.ufov_processed)*100:.1f}%")
        
        # 参加者別の統計
        participant_stats = self.matches_df.groupby('participant_id').size()
        print(f"\nParticipants updated: {len(participant_stats)}")
        print(f"Records per participant: {participant_stats.mean():.1f} (avg)")
        
        # 測定波別の詳細統計
        print(f"\nDetailed statistics by measurement wave:")
        wave_stats = self.matches_df.groupby('measurement_wave').agg({
            'participant_id': 'nunique',
            'name': 'count',
            'date_diff': ['mean', 'max']
        }).round(1)
        
        for wave in wave_stats.index:
            participants = wave_stats.loc[wave, ('participant_id', 'nunique')]
            records = wave_stats.loc[wave, ('name', 'count')]
            avg_diff = wave_stats.loc[wave, ('date_diff', 'mean')]
            max_diff = wave_stats.loc[wave, ('date_diff', 'max')]
            print(f"  Wave {wave}: {records} records, {participants} participants, avg date diff: {avg_diff} days, max: {max_diff} days")
        
        # 日付マッチング統計
        print(f"\nDate matching statistics:")
        print(f"Perfect matches (same day): {(self.matches_df['date_diff'] == 0).sum()}")
        print(f"Close matches (1-3 days): {((self.matches_df['date_diff'] >= 1) & (self.matches_df['date_diff'] <= 3)).sum()}")
        print(f"Medium matches (4-7 days): {((self.matches_df['date_diff'] >= 4) & (self.matches_df['date_diff'] <= 7)).sum()}")
        print(f"Loose matches (8-15 days): {((self.matches_df['date_diff'] >= 8) & (self.matches_df['date_diff'] <= 15)).sum()}")
        
        # コホート別統計
        if 'cohort' in self.matches_df.columns:
            cohort_stats = self.matches_df.groupby('cohort').size()
            print(f"\nMatches by cohort:")
            for cohort, count in cohort_stats.items():
                print(f"  {cohort}: {count} records")
    
    def run_integration(self):
        """
        データ統合プロセス全体を実行
        """
        print("Starting UFOV data integration process...")
        print("="*50)
        
        # 1. データ読み込み
        if not self.load_data():
            return False
        
        # 2. UFOVデータの前処理
        self.preprocess_ufov_data()
        
        # 3. 氏名マッピングの作成
        self.create_name_mapping()
        
        # 4. データマッチング
        self.match_data_by_date_and_name()
        
        # 5. masterデータの更新
        self.update_master_data()
        
        # 6. レポート生成
        self.generate_report()
        
        # 7. データ保存（自動的にバックアップも作成）
        self.save_updated_data()
        
        return True

# 使用例
if __name__ == "__main__":
    # esports-analysis/dataディレクトリで実行
    integrator = UFOVDataIntegrator("data")
    integrator.run_integration()