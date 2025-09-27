import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import os
warnings.filterwarnings('ignore')

# 統計モデル
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.stats.anova import anova_lm

# 可視化の拡張
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 日本語フォント設定
plt.rcParams['font.family'] = 'DejaVu Sans'
sns.set_style("whitegrid")
sns.set_palette("husl")

def setup_output_directory():
    """出力ディレクトリの作成"""
    output_dir = "analysis_result/lmm_result"
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 出力ディレクトリを作成: {output_dir}")
    return output_dir

def load_preprocessed_data():
    """
    前処理済みデータの読み込み
    data_overview.pyで作成されたデータを想定
    """
    print("=== データの読み込み ===")
    try:
        # メインデータ読み込み（パスは適宜調整）
        df = pd.read_excel('./data/data_master.xlsx', sheet_name='master')
        
        # cohort 2024_G1の抽出と前処理
        df = df[df['cohort'] == '2024_G1'].copy()
        df.reset_index(drop=True, inplace=True)
        
        # measurement_waveのセンタリング (1,2,3 -> 0,1,2)
        print("🔄 measurement_waveをセンタリング (1,2,3 -> 0,1,2)")
        df['measurement_wave'] = df['measurement_wave'] - 1
        
        # コース分類
        df['course_group'] = df['course'].map({  # type: ignore
            'eスポーツエデュケーションコース': 'eSports',
            'リベラルアーツコース': 'Liberal Arts'
        })
        
        print(f"データ形状: {df.shape}")
        print(f"参加者数: {df['participant_id'].nunique()}名")  # type: ignore
        print(f"測定時期: {sorted(df['measurement_wave'].unique())}")  # type: ignore
        
        return df
        
    except FileNotFoundError:
        print("データファイルが見つかりません。パスを確認してください。")
        return None

def convert_tmt_units(df):
    """
    TMT関連変数の単位変換（ミリ秒→秒）
    """
    print("\n🔄 TMT単位変換: ミリ秒 → 秒")
    print("-" * 40)
    
    # TMTの時間変数を特定
    tmt_time_vars = [col for col in df.columns if 'tmt' in col.lower() and 'time' in col.lower()]
    
    # 具体的なTMT時間変数名（データに応じて調整）
    possible_tmt_vars = [
        'tmt_combined_trailtime',
        'tmt_a_time', 'tmt_b_time',
        'tmt_trailtime_a', 'tmt_trailtime_b',
        'tmt_time_total', 'tmt_completion_time'
    ]
    
    converted_vars = []
    
    for var in possible_tmt_vars:
        if var in df.columns:
            # 変換前の統計情報
            original_data = df[var].dropna()
            if len(original_data) > 0:
                print(f"\n📊 {var}:")
                print(f"  変換前 - 平均: {original_data.mean():.1f}ms, 範囲: {original_data.min():.1f}-{original_data.max():.1f}ms")
                
                # ミリ秒から秒に変換
                df[var] = df[var] / 1000.0
                
                # 変換後の統計情報
                converted_data = df[var].dropna()
                print(f"  変換後 - 平均: {converted_data.mean():.2f}s, 範囲: {converted_data.min():.2f}-{converted_data.max():.2f}s")
                
                converted_vars.append(var)
    
    if len(converted_vars) > 0:
        print(f"\n✅ {len(converted_vars)}個のTMT変数を秒単位に変換: {converted_vars}")
    else:
        print("\n⚠️ TMT時間変数が見つかりませんでした。変数名を確認してください。")
        print(f"データに含まれるTMT関連変数: {[col for col in df.columns if 'tmt' in col.lower()]}")
    
    return df, converted_vars

def define_analysis_variables():
    """分析対象変数の定義"""
    
    # 認知スキル変数（12個）
    cognitive_vars = [
        'corsi_ncorrect_total',      # Corsi正答数
        'corsi_blockspan',           # Corsiブロックスパン
        'corsi_totalscore',          # Corsi総得点
        'fourchoice_prop_correct',   # 四択正答率
        'fourchoice_mean_rt',        # 四択反応時間
        'stroop_propcorrect',        # Stroop正答率
        'stroop_mean_rt',            # Stroop反応時間
        'tmt_combined_errors',       # TMTエラー数
        'tmt_combined_trailtime',    # TMT完了時間（秒単位）
        'ufov_subtest1_threshold',   # UFOV閾値1
        'ufov_subtest2_threshold',   # UFOV閾値2
        'ufov_subtest3_threshold'    # UFOV閾値3
    ]
    
    # 非認知スキル変数（13個）
    non_cognitive_vars = [
        'bigfive_extraversion',      # ビッグファイブ：外向性
        'bigfive_agreeableness',     # ビッグファイブ：協調性
        'bigfive_conscientiousness', # ビッグファイブ：誠実性
        'bigfive_neuroticism',       # ビッグファイブ：神経症傾向
        'bigfive_openness',          # ビッグファイブ：開放性
        'grit_total',                # GRIT総得点
        'mindset_total',             # マインドセット総得点
        'ct_logical_awareness',      # 批判的思考：論理的気づき
        'ct_inquiry',                # 批判的思考：探究心
        'ct_objectivity',            # 批判的思考：客観性
        'ct_evidence_based',         # 批判的思考：根拠重視
        'who5_total',                # WHO-5ウェルビーイング
        'swbs_total'                 # 主観的ウェルビーイング
    ]
    
    # 有意な効果があった変数（優先分析）
    significant_vars = [
        # コース効果あり
        'fourchoice_mean_rt', 'tmt_combined_errors', 'tmt_combined_trailtime', 
        'ufov_subtest3_threshold', 'bigfive_extraversion', 'bigfive_openness',
        'ct_logical_awareness', 'ct_evidence_based',
        # 時間効果あり
        'corsi_ncorrect_total', 'corsi_blockspan', 'corsi_totalscore'
    ]
    
    # 全分析変数
    all_analysis_vars = cognitive_vars + non_cognitive_vars
    
    return {
        'cognitive': cognitive_vars,
        'non_cognitive': non_cognitive_vars,
        'significant': significant_vars,
        'all': all_analysis_vars
    }

def run_basic_lmm(df, variable, verbose=True):
    """
    基本的なLMM分析（ランダム切片モデル）
    
    Parameters:
    -----------
    df : pd.DataFrame
        分析データ
    variable : str
        従属変数名
    verbose : bool
        詳細出力するかどうか
    """
    
    # 欠損値のある行を除外
    analysis_data = df[['participant_id', 'course_group', 'measurement_wave', variable]].dropna()
    
    if len(analysis_data) == 0:
        print(f"❌ {variable}: 分析可能なデータがありません")
        return None
    
    try:
        # ランダム切片モデル
        formula = f"{variable} ~ C(course_group) * measurement_wave"
        model = smf.mixedlm(formula, analysis_data, groups=analysis_data["participant_id"])
        result = model.fit()
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"📊 {variable} - ランダム切片モデル")
            print(f"{'='*60}")
            print(f"サンプルサイズ: {len(analysis_data)}観測, {analysis_data['participant_id'].nunique()}名")
            print(f"欠損処理: {len(df) - len(analysis_data)}観測を除外")
            
            # 単位情報の表示
            if 'tmt' in variable.lower() and 'time' in variable.lower():
                print(f"📏 単位: 秒 (seconds)")
            elif 'rt' in variable.lower():
                print(f"📏 単位: ミリ秒 (milliseconds)")
            
            print("\n固定効果:")
            print(result.summary().tables[1])
            
            # 効果の解釈
            interpret_lmm_results(result, variable)
        
        return result
        
    except Exception as e:
        print(f"❌ {variable}: LMM分析でエラー - {str(e)}")
        return None

def run_random_slope_lmm(df, variable, verbose=True):
    """
    ランダム傾きモデル（個人の成長速度差を考慮）
    
    Parameters:
    -----------
    df : pd.DataFrame
        分析データ
    variable : str
        従属変数名
    verbose : bool
        詳細出力するかどうか
    """
    
    analysis_data = df[['participant_id', 'course_group', 'measurement_wave', variable]].dropna()
    
    if len(analysis_data) == 0:
        print(f"❌ {variable}: 分析可能なデータがありません")
        return None
    
    try:
        # ランダム傾きモデル
        formula = f"{variable} ~ C(course_group) * measurement_wave"
        model = smf.mixedlm(formula, analysis_data, 
                           groups=analysis_data["participant_id"],
                           re_formula="~ measurement_wave")
        result = model.fit()
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"📈 {variable} - ランダム傾きモデル")
            print(f"{'='*60}")
            print(f"サンプルサイズ: {len(analysis_data)}観測, {analysis_data['participant_id'].nunique()}名")
            
            # 単位情報の表示
            if 'tmt' in variable.lower() and 'time' in variable.lower():
                print(f"📏 単位: 秒 (seconds)")
            elif 'rt' in variable.lower():
                print(f"📏 単位: ミリ秒 (milliseconds)")
            
            print("\n固定効果:")
            print(result.summary().tables[1])
            
            # ランダム効果の分散
            print(f"\nRandom Effects Variance:")
            print(f"Individual differences (intercept): {result.cov_re.iloc[0,0]:.4f}")
            if result.cov_re.shape[0] > 1:
                print(f"Growth rate differences (slope): {result.cov_re.iloc[1,1]:.4f}")
                print(f"Intercept-slope correlation: {result.cov_re.iloc[0,1]/np.sqrt(result.cov_re.iloc[0,0]*result.cov_re.iloc[1,1]):.4f}")
        
        return result
        
    except Exception as e:
        print(f"❌ {variable}: ランダム傾きモデルでエラー - {str(e)}")
        return None

def compare_models(df, variable):
    """
    モデル比較（ランダム切片 vs ランダム傾き）
    """
    print(f"\n🔍 {variable} - モデル比較")
    print("-" * 50)
    
    # 両モデルを実行
    model1 = run_basic_lmm(df, variable, verbose=False)
    model2 = run_random_slope_lmm(df, variable, verbose=False)
    
    if model1 is None or model2 is None:
        print("モデル比較できません")
        return
    
    # AIC/BIC比較
    print(f"ランダム切片モデル - AIC: {model1.aic:.2f}, BIC: {model1.bic:.2f}")
    print(f"ランダム傾きモデル - AIC: {model2.aic:.2f}, BIC: {model2.bic:.2f}")
    
    # より良いモデルの判定
    if model2.aic < model1.aic:
        print("✅ Random slope model is superior (individual growth rate differences are important)")
    else:
        print("✅ Random intercept model is sufficient (individual growth rate differences are small)")
    
    return model1, model2

def interpret_lmm_results(result, variable):
    """LMM結果の解釈（TMT単位考慮版・センタリング版）"""
    
    try:
        # 固定効果の抽出
        coef_table = result.summary().tables[1]
        params = result.params
        pvalues = result.pvalues
        
        print(f"\n💡 {variable}の結果解釈:")
        print("-" * 40)
        
        # 単位情報
        unit_info = ""
        if 'tmt' in variable.lower() and 'time' in variable.lower():
            unit_info = " (seconds)"
        elif 'rt' in variable.lower():
            unit_info = " (milliseconds)"
        
        # コース効果
        if 'C(course_group)[T.Liberal Arts]' in params:
            course_coef = params['C(course_group)[T.Liberal Arts]']
            course_p = pvalues['C(course_group)[T.Liberal Arts]']
            
            if course_p < 0.05:
                # TMT課題と反応時間は短い方が良い
                if 'tmt_combined_trailtime' in variable or 'rt' in variable:
                    if course_coef > 0:
                        comparison = f"Liberal Arts is SLOWER than eSports (+{abs(course_coef):.3f}{unit_info} worse)"
                    else:
                        comparison = f"eSports is SLOWER than Liberal Arts (+{abs(course_coef):.3f}{unit_info} worse for eSports)"
                # エラー数は少ない方が良い
                elif 'errors' in variable:
                    if course_coef > 0:
                        comparison = f"Liberal Arts has MORE errors than eSports (+{abs(course_coef):.3f} worse)"
                    else:
                        comparison = f"eSports has MORE errors than Liberal Arts (+{abs(course_coef):.3f} worse for eSports)"
                # 一般的な指標は高い方が良い
                else:
                    if course_coef > 0:
                        comparison = f"Liberal Arts is BETTER than eSports (+{abs(course_coef):.3f}{unit_info})"
                    else:
                        comparison = f"eSports is BETTER than Liberal Arts (+{abs(course_coef):.3f}{unit_info} for eSports)"
                
                print(f"🎯 Course Effect: {comparison} (p={course_p:.4f})")
                print(f"   📊 解釈: Wave1(0)時点でのコース間の差")
            else:
                print(f"🎯 Course Effect: No significant difference (p={course_p:.4f})")
        
        # 時間効果（センタリング後: 0=Wave1, 1=Wave2, 2=Wave3）
        if 'measurement_wave' in params:
            time_coef = params['measurement_wave']
            time_p = pvalues['measurement_wave']
            
            if time_p < 0.05:
                # TMT完了時間、反応時間、エラー数は減少が良い
                if 'tmt_combined_trailtime' in variable or 'rt' in variable or 'errors' in variable:
                    if time_coef < 0:
                        direction = f"IMPROVEMENT: decrease of {abs(time_coef):.3f}{unit_info} per wave"
                    else:
                        direction = f"DETERIORATION: increase of {abs(time_coef):.3f}{unit_info} per wave"
                # 一般的な指標は増加が良い
                else:
                    if time_coef > 0:
                        direction = f"IMPROVEMENT: increase of {abs(time_coef):.3f}{unit_info} per wave"
                    else:
                        direction = f"DETERIORATION: decrease of {abs(time_coef):.3f}{unit_info} per wave"
                
                print(f"⏰ Time Effect: {direction} (p={time_p:.4f})")
                print(f"   📊 解釈: Wave1(0)からWave3(2)まで、1回の測定ごとに{abs(time_coef):.3f}{unit_info}の変化")
            else:
                print(f"⏰ Time Effect: No significant change (p={time_p:.4f})")
        
        # 交互作用
        interaction_key = 'C(course_group)[T.Liberal Arts]:measurement_wave'
        if interaction_key in params:
            int_coef = params[interaction_key]
            int_p = pvalues[interaction_key]
            
            if int_p < 0.05:
                print(f"🔄 Interaction: Time changes DIFFERENTLY between courses ({int_coef:+.3f}{unit_info} difference in slope, p={int_p:.4f})")
            else:
                print(f"🔄 Interaction: Time changes SIMILARLY between courses (p={int_p:.4f})")
                
    except Exception as e:
        print(f"結果解釈でエラー: {str(e)}")

def visualize_individual_trajectories(df, variable, output_dir):
    """
    個人軌跡の可視化（TMT単位考慮版）
    """
    
    # データ準備
    plot_data = df[['participant_id', 'course_group', 'measurement_wave', variable]].dropna()
    
    if len(plot_data) == 0:
        print(f"❌ {variable}: 可視化用データがありません")
        return
    
    # 単位情報
    y_label = variable
    if 'tmt' in variable.lower() and 'time' in variable.lower():
        y_label += " (seconds)"
    elif 'rt' in variable.lower():
        y_label += " (milliseconds)"
    
    # Plotlyでインタラクティブ可視化
    fig = px.line(plot_data, 
                  x='measurement_wave', 
                  y=variable,
                  color='course_group',
                  line_group='participant_id',
                  title=f'{variable} - Individual Trajectories',
                  labels={'measurement_wave': 'Experiment Number', 
                         'course_group': 'Course',
                         variable: y_label})
    
    # 群平均も追加
    mean_data = plot_data.groupby(['course_group', 'measurement_wave'])[variable].mean().reset_index()
    
    for course in mean_data['course_group'].unique():
        course_data = mean_data[mean_data['course_group'] == course]
        fig.add_trace(go.Scatter(x=course_data['measurement_wave'], 
                                y=course_data[variable],
                                mode='lines+markers',
                                name=f'{course} (Mean)',
                                line=dict(width=4)))
    
    # X軸を整数のみに設定（センタリング後）
    fig.update_xaxes(
        tickvals=[0, 1, 2],
        ticktext=['1', '2', '3'],
        title='Experiment Number'
    )
    
    # Y軸ラベル更新
    fig.update_yaxes(title=y_label)
    
    fig.update_layout(height=600, showlegend=True)
    
    # ファイル保存
    save_path = os.path.join(output_dir, f"trajectory_{variable}.html")
    fig.write_html(save_path)
    print(f"📊 {variable}の軌跡図を保存: {save_path}")
    
    return fig

def create_comprehensive_lmm_summary(df, variables, output_dir):
    """
    全変数のLMM結果サマリー作成（TMT単位修正版）
    """
    
    print("\n" + "="*80)
    print("📊 包括的LMM分析サマリー - 全25変数 (TMT単位修正版)")
    print("="*80)
    
    summary_results = []
    
    # 認知スキル分析
    print(f"\n🧠 認知スキル変数の分析 ({len(variables['cognitive'])}変数)")
    print("-" * 60)
    
    for var in variables['cognitive']:
        result = run_basic_lmm(df, var, verbose=False)
        if result is not None:
            summary_results.append(extract_lmm_summary(result, var, 'cognitive'))
    
    # 非認知スキル分析
    print(f"\n💭 非認知スキル変数の分析 ({len(variables['non_cognitive'])}変数)")
    print("-" * 60)
    
    for var in variables['non_cognitive']:
        result = run_basic_lmm(df, var, verbose=False)
        if result is not None:
            summary_results.append(extract_lmm_summary(result, var, 'non_cognitive'))
    
    # 結果をDataFrameに変換
    summary_df = pd.DataFrame(summary_results)
    
    if len(summary_df) > 0:
        # 結果の整理と表示
        display_lmm_summary_table(summary_df)
        save_lmm_results(summary_df, output_dir)
    
    return summary_df

def extract_lmm_summary(result, variable, category):
    """
    LMM結果からサマリー情報を抽出
    """
    try:
        params = result.params
        pvalues = result.pvalues
        
        # コース効果
        course_coef = params.get('C(course_group)[T.Liberal Arts]', np.nan)
        course_p = pvalues.get('C(course_group)[T.Liberal Arts]', np.nan)
        
        # 時間効果
        time_coef = params.get('measurement_wave', np.nan)
        time_p = pvalues.get('measurement_wave', np.nan)
        
        # 交互作用効果
        interaction_coef = params.get('C(course_group)[T.Liberal Arts]:measurement_wave', np.nan)
        interaction_p = pvalues.get('C(course_group)[T.Liberal Arts]:measurement_wave', np.nan)
        
        # 単位情報
        unit = ""
        if 'tmt' in variable.lower() and 'time' in variable.lower():
            unit = "seconds"
        elif 'rt' in variable.lower():
            unit = "milliseconds"
        
        return {
            'Variable': variable,
            'Category': category,
            'Unit': unit,
            'Course_Coef': course_coef,
            'Course_P': course_p,
            'Course_Sig': '***' if course_p < 0.001 else '**' if course_p < 0.01 else '*' if course_p < 0.05 else 'ns',
            'Time_Coef': time_coef,
            'Time_P': time_p,
            'Time_Sig': '***' if time_p < 0.001 else '**' if time_p < 0.01 else '*' if time_p < 0.05 else 'ns',
            'Interaction_Coef': interaction_coef,
            'Interaction_P': interaction_p,
            'Interaction_Sig': '***' if interaction_p < 0.001 else '**' if interaction_p < 0.01 else '*' if interaction_p < 0.05 else 'ns',
            'AIC': result.aic,
            'BIC': result.bic,
            'Log_Likelihood': result.llf
        }
        
    except Exception as e:
        print(f"⚠️ {variable}: サマリー抽出エラー - {str(e)}")
        return None

def display_lmm_summary_table(summary_df):
    """
    LMM結果サマリーテーブルの表示（TMT単位考慮版）
    """
    
    print(f"\n📋 LMM分析結果サマリー (TMT単位修正版)")
    print("="*100)
    
    # 有意な効果のカウント
    course_sig = (summary_df['Course_P'] < 0.05).sum()
    time_sig = (summary_df['Time_P'] < 0.05).sum()
    interaction_sig = (summary_df['Interaction_P'] < 0.05).sum()
    
    print(f"有意な効果 (p < 0.05):")
    print(f"  コース効果: {course_sig}/{len(summary_df)}変数")
    print(f"  時間効果: {time_sig}/{len(summary_df)}変数")
    print(f"  交互作用: {interaction_sig}/{len(summary_df)}変数")
    
    # カテゴリ別サマリー
    print(f"\n📊 カテゴリ別サマリー:")
    for category in ['cognitive', 'non_cognitive']:
        cat_data = summary_df[summary_df['Category'] == category]
        if len(cat_data) > 0:
            cat_course_sig = (cat_data['Course_P'] < 0.05).sum()
            cat_time_sig = (cat_data['Time_P'] < 0.05).sum()
            
            category_name = '認知スキル' if category == 'cognitive' else '非認知スキル'
            print(f"  {category_name}: コース効果{cat_course_sig}/{len(cat_data)}, 時間効果{cat_time_sig}/{len(cat_data)}")
    
    # 有意な効果のある変数をリスト表示（単位表示付き）
    print(f"\n🎯 有意なコース効果のある変数:")
    course_vars = summary_df[summary_df['Course_P'] < 0.05].sort_values('Course_P')
    for _, row in course_vars.iterrows():
        unit_str = f" ({row['Unit']})" if row['Unit'] else ""
        if 'tmt_combined_trailtime' in row['Variable'] or 'rt' in row['Variable']:
            direction = "Liberal Arts SLOWER" if row['Course_Coef'] > 0 else "eSports SLOWER"
        elif 'errors' in row['Variable']:
            direction = "Liberal Arts MORE errors" if row['Course_Coef'] > 0 else "eSports MORE errors"
        else:
            direction = "Liberal Arts > eSports" if row['Course_Coef'] > 0 else "eSports > Liberal Arts"
        
        print(f"  {row['Variable']}{unit_str} ({row['Category']}): p={row['Course_P']:.4f} {row['Course_Sig']} [{direction}]")
    
    print(f"\n⏰ 有意な時間効果のある変数:")
    time_vars = summary_df[summary_df['Time_P'] < 0.05].sort_values('Time_P')
    for _, row in time_vars.iterrows():
        unit_str = f" ({row['Unit']})" if row['Unit'] else ""
        if 'tmt_combined_trailtime' in row['Variable'] or 'rt' in row['Variable'] or 'errors' in row['Variable']:
            direction = "improvement (decrease)" if row['Time_Coef'] < 0 else "deterioration (increase)"
        else:
            direction = "improvement (increase)" if row['Time_Coef'] > 0 else "deterioration (decrease)"
        
        print(f"  {row['Variable']}{unit_str} ({row['Category']}): p={row['Time_P']:.4f} {row['Time_Sig']} [{direction}]")
    
    if interaction_sig > 0:
        print(f"\n🔄 有意な交互作用のある変数:")
        int_vars = summary_df[summary_df['Interaction_P'] < 0.05].sort_values('Interaction_P')
        for _, row in int_vars.iterrows():
            unit_str = f" ({row['Unit']})" if row['Unit'] else ""
            print(f"  {row['Variable']}{unit_str} ({row['Category']}): p={row['Interaction_P']:.4f} {row['Interaction_Sig']}")

def save_lmm_results(summary_df, output_dir):
    """
    LMM結果をExcelファイルに保存（TMT単位情報付き）
    """
    try:
        # メインの結果保存
        excel_path = os.path.join(output_dir, "lmm_results_comprehensive.xlsx")
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # 全結果シート
            summary_df.to_excel(writer, sheet_name='全結果サマリー', index=False)
            
            # 有意な効果別シート
            course_sig = summary_df[summary_df['Course_P'] < 0.05].sort_values('Course_P')
            if len(course_sig) > 0:
                course_sig.to_excel(writer, sheet_name='有意なコース効果', index=False)
            
            time_sig = summary_df[summary_df['Time_P'] < 0.05].sort_values('Time_P')
            if len(time_sig) > 0:
                time_sig.to_excel(writer, sheet_name='有意な時間効果', index=False)
            
            interaction_sig = summary_df[summary_df['Interaction_P'] < 0.05].sort_values('Interaction_P')
            if len(interaction_sig) > 0:
                interaction_sig.to_excel(writer, sheet_name='有意な交互作用', index=False)
            
            # カテゴリ別シート
            cognitive_data = summary_df[summary_df['Category'] == 'cognitive']
            cognitive_data.to_excel(writer, sheet_name='認知スキル結果', index=False)
            
            non_cognitive_data = summary_df[summary_df['Category'] == 'non_cognitive']
            non_cognitive_data.to_excel(writer, sheet_name='非認知スキル結果', index=False)
        
        print(f"💾 LMM結果をExcelで保存: {excel_path}")
        return excel_path
        
    except Exception as e:
        print(f"⚠️ Excel保存エラー: {str(e)}")
        return None

def run_detailed_analysis_for_significant_vars(df, summary_df):
    """
    有意な効果のあった変数の詳細分析（TMT単位考慮版）
    """
    
    # 有意な効果のある変数を特定
    significant_vars = summary_df[
        (summary_df['Course_P'] < 0.05) | 
        (summary_df['Time_P'] < 0.05) | 
        (summary_df['Interaction_P'] < 0.05)
    ]['Variable'].tolist()
    
    print(f"\n🔍 有意な効果のあった{len(significant_vars)}変数の詳細分析 (TMT単位修正版)")
    print("="*60)
    
    detailed_results = {}
    
    for var in significant_vars:
        print(f"\n--- {var} 詳細分析 ---")
        
        # ランダム切片モデル
        basic_model = run_basic_lmm(df, var, verbose=True)
        
        # ランダム傾きモデルも試行
        try:
            slope_model = run_random_slope_lmm(df, var, verbose=False)
            if slope_model is not None and basic_model is not None:
                print(f"Model comparison - Intercept AIC: {basic_model.aic:.2f}, Slope AIC: {slope_model.aic:.2f}")
                if slope_model.aic < basic_model.aic:
                    print("✅ Random slope model is superior")
                    detailed_results[var] = slope_model
                else:
                    print("✅ Random intercept model is sufficient")
                    detailed_results[var] = basic_model
            else:
                if basic_model is not None:
                    detailed_results[var] = basic_model
        except:
            if basic_model is not None:
                detailed_results[var] = basic_model
    
    return detailed_results

def create_static_visualizations(df, variables, output_dir):
    """
    静的グラフの作成（PNG保存）TMT単位考慮版
    """
    
    print(f"\n📈 静的グラフの作成 (TMT単位修正版)")
    print("-" * 40)
    
    # グラフ保存用ディレクトリ
    graph_dir = os.path.join(output_dir, "graphs")
    os.makedirs(graph_dir, exist_ok=True)
    
    # 1. 群平均比較グラフ（エラーバー付き）- 全変数対象
    create_group_mean_plots(df, variables['all'], graph_dir)
    
    # 2. 効果サイズ可視化
    create_effect_size_plots(df, variables['all'], graph_dir)
    
    # 3. カテゴリ別サマリーグラフ
    create_category_summary_plots(df, variables, graph_dir)
    
    print(f"📊 静的グラフを保存: {graph_dir}/")

def create_group_mean_plots(df, variables, graph_dir):
    """
    群平均比較グラフ（エラーバー付き）TMT単位考慮版
    """
    
    print(f"  群平均比較グラフを作成中... (全{len(variables)}変数)")
    
    created_count = 0
    for i, var in enumerate(variables, 1):
        try:
            plot_data = df[['course_group', 'measurement_wave', var]].dropna()
            if len(plot_data) == 0:
                print(f"    ⚠️ {i}/{len(variables)} {var}: データなし")
                continue
                
            # 群平均とSE計算
            summary_stats = plot_data.groupby(['course_group', 'measurement_wave'])[var].agg([
                'mean', 'std', 'count'
            ]).reset_index()
            summary_stats['se'] = summary_stats['std'] / np.sqrt(summary_stats['count'])
            
            # matplotlib図の作成
            plt.figure(figsize=(10, 6))
            
            # コースごとの色を指定
            course_colors = {
                'eSports': '#1C1C7C',         # 濃い紺色
                'Liberal Arts': '#E69F00'    # 濃いオレンジ
            }
            
            for course in summary_stats['course_group'].unique():
                course_data = summary_stats[summary_stats['course_group'] == course]
                plt.errorbar(course_data['measurement_wave'], 
                           course_data['mean'],
                           yerr=course_data['se'],
                           marker='o', linewidth=2, markersize=8,
                           label=course, capsize=5,
                           color=course_colors.get(course, None))
            
            # X軸を整数のみに設定（センタリング後）
            plt.xticks([0, 1, 2], ['1', '2', '3'])
            plt.xlabel('Experiment Number', fontsize=14)
            
            # Y軸ラベルに単位情報を追加
            y_label = var
            if 'tmt' in var.lower() and 'time' in var.lower():
                y_label += " (seconds)"
            elif 'rt' in var.lower():
                y_label += " (milliseconds)"
            
            plt.ylabel(y_label, fontsize=14)
            
            # bigfive変数の場合、日本語タイトルを使用
            if 'bigfive' in var.lower():
                if 'extraversion' in var.lower():
                    title = 'Big Five: Extraversion - Group Mean Comparison'
                elif 'agreeableness' in var.lower():
                    title = 'Big Five: Agreeableness - Group Mean Comparison'
                elif 'conscientiousness' in var.lower():
                    title = 'Big Five: Conscientiousness - Group Mean Comparison'
                elif 'neuroticism' in var.lower():
                    title = 'Big Five: Neuroticism - Group Mean Comparison'
                elif 'openness' in var.lower():
                    title = 'Big Five: Openness - Group Mean Comparison'
                else:
                    title = f'{var} - Group Mean Comparison'
            else:
                title = f'{var} - Group Mean Comparison'
            
            plt.title(title, fontsize=18, fontweight='bold')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # bigfive変数の場合、Y軸の範囲を4-12に固定
            if 'bigfive' in var.lower():
                # データの範囲を取得（参考用）
                y_min = plot_data[var].min()
                y_max = plot_data[var].max()
                
                # Y軸を4-12の範囲に固定
                plt.ylim(4, 12)
                
                print(f"    📏 {var}: Y軸範囲固定 - データ範囲: {y_min:.2f} ~ {y_max:.2f} → 表示範囲: 4.0 ~ 12.0")
            
            plt.tight_layout()
            
            # PNG保存
            save_path = os.path.join(graph_dir, f"group_mean_{var}.png")
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            created_count += 1
            print(f"    ✅ {i}/{len(variables)} {var}: グラフ作成完了")
            
        except Exception as e:
            print(f"    ⚠️ {i}/{len(variables)} {var}: エラー - {str(e)}")
    
    print(f"    📊 群平均比較グラフ作成完了: {created_count}/{len(variables)}変数")

def create_effect_size_plots(df, variables, graph_dir):
    """
    効果サイズの可視化（TMT単位考慮版）
    """
    
    print("  効果サイズグラフを作成中...")
    
    try:
        effect_sizes = []
        
        for var in variables:
            analysis_data = df[['participant_id', 'course_group', 'measurement_wave', var]].dropna()
            if len(analysis_data) == 0:
                continue
            
            # Wave1とWave3でのコース間効果サイズ（Cohen's d）
            for wave in [0, 2]:  # センタリング後: 0=Wave1, 2=Wave3
                wave_data = analysis_data[analysis_data['measurement_wave'] == wave]
                if len(wave_data) < 10:  # 最小サンプルサイズ
                    continue
                    
                esports = wave_data[wave_data['course_group'] == 'eSports'][var]
                liberal = wave_data[wave_data['course_group'] == 'Liberal Arts'][var]
                
                if len(esports) > 0 and len(liberal) > 0:
                    # Cohen's d計算
                    pooled_std = np.sqrt(((len(esports)-1)*esports.var() + 
                                        (len(liberal)-1)*liberal.var()) / 
                                       (len(esports)+len(liberal)-2))
                    cohens_d = (esports.mean() - liberal.mean()) / pooled_std
                    
                    # TMT時間課題と反応時間は符号を反転（短い方が良い）
                    if 'tmt' in var.lower() and 'time' in var.lower():
                        cohens_d = -cohens_d  # TMT時間は短い方が良いので符号反転
                    elif 'rt' in var.lower():
                        cohens_d = -cohens_d  # 反応時間は短い方が良いので符号反転
                    elif 'errors' in var.lower():
                        cohens_d = -cohens_d  # エラー数は少ない方が良いので符号反転
                    
                    effect_sizes.append({
                        'Variable': var,
                        'Wave': f'Experiment {wave + 1}',  # センタリング後: 0→1, 2→3
                        'Cohens_d': cohens_d,
                        'Category': 'cognitive' if var in ['corsi_ncorrect_total', 'corsi_blockspan', 'corsi_totalscore',
                                                          'fourchoice_prop_correct', 'fourchoice_mean_rt',
                                                          'stroop_propcorrect', 'stroop_mean_rt',
                                                          'tmt_combined_errors', 'tmt_combined_trailtime',
                                                          'ufov_subtest1_threshold', 'ufov_subtest2_threshold', 'ufov_subtest3_threshold'] else 'non_cognitive'
                    })
        
        if len(effect_sizes) > 0:
            effect_df = pd.DataFrame(effect_sizes)
            
            # Wave1とWave3の効果サイズ比較
            plt.figure(figsize=(12, 8))
            
            # サブプロット作成
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
            
            for i, experiment in enumerate(['Experiment 1', 'Experiment 3']):
                experiment_data = effect_df[effect_df['Wave'] == experiment]
                
                cognitive = experiment_data[experiment_data['Category'] == 'cognitive']['Cohens_d']
                non_cognitive = experiment_data[experiment_data['Category'] == 'non_cognitive']['Cohens_d']
                
                ax = ax1 if i == 0 else ax2
                
                # バイオリンプロット
                if len(cognitive) > 0:
                    parts1 = ax.violinplot([cognitive], positions=[1], widths=0.6, 
                                         showmeans=True, showmedians=True)
                    parts1['bodies'][0].set_facecolor('lightblue')
                    parts1['bodies'][0].set_alpha(0.7)
                
                if len(non_cognitive) > 0:
                    parts2 = ax.violinplot([non_cognitive], positions=[2], widths=0.6,
                                         showmeans=True, showmedians=True)
                    parts2['bodies'][0].set_facecolor('lightcoral')
                    parts2['bodies'][0].set_alpha(0.7)
                
                ax.set_xticks([1, 2])
                ax.set_xticklabels(['Cognitive Skills', 'Non-Cognitive Skills'], fontsize=12)
                ax.set_ylabel("Cohen's d (eSports favoring)", fontsize=14)
                ax.set_title(f'{experiment} - Effect Size Distribution', fontsize=16, fontweight='bold')
                ax.grid(True, alpha=0.3)
                ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
                
                # 効果サイズの解釈線
                ax.axhline(y=0.2, color='green', linestyle=':', alpha=0.5, label='Small Effect')
                ax.axhline(y=0.5, color='orange', linestyle=':', alpha=0.5, label='Medium Effect')
                ax.axhline(y=0.8, color='red', linestyle=':', alpha=0.5, label='Large Effect')
                ax.axhline(y=-0.2, color='green', linestyle=':', alpha=0.5)
                ax.axhline(y=-0.5, color='orange', linestyle=':', alpha=0.5)
                ax.axhline(y=-0.8, color='red', linestyle=':', alpha=0.5)
                
                if i == 0:
                    ax.legend()
            
            plt.tight_layout()
            save_path = os.path.join(graph_dir, "effect_sizes_comparison.png")
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
    except Exception as e:
        print(f"    ⚠️ 効果サイズグラフ: エラー - {str(e)}")

def create_category_summary_plots(df, variables, graph_dir):
    """
    カテゴリ別サマリーグラフ（TMT単位考慮版）
    """
    
    print("  カテゴリ別サマリーグラフを作成中...")
    
    try:
        # 認知・非認知別の改善度計算
        improvement_data = []
        
        for category, var_list in [('Cognitive Skills', variables['cognitive']), 
                                  ('Non-Cognitive Skills', variables['non_cognitive'])]:
            for var in var_list:
                analysis_data = df[['participant_id', 'course_group', 'measurement_wave', var]].dropna()
                
                # 個人の改善度計算（Wave3 - Wave1）
                for participant in analysis_data['participant_id'].unique():
                    p_data = analysis_data[analysis_data['participant_id'] == participant]
                    
                    wave1_data = p_data[p_data['measurement_wave'] == 0]  # センタリング後: 0=Wave1
                    wave3_data = p_data[p_data['measurement_wave'] == 2]  # センタリング後: 2=Wave3
                    
                    if len(wave1_data) == 1 and len(wave3_data) == 1:
                        improvement = wave3_data[var].iloc[0] - wave1_data[var].iloc[0]
                        
                        # TMT時間、反応時間、エラー数は符号を反転（減少が改善）
                        if 'tmt' in var.lower() and 'time' in var.lower():
                            improvement = -improvement
                        elif 'rt' in var.lower():
                            improvement = -improvement
                        elif 'errors' in var.lower():
                            improvement = -improvement
                        
                        course = wave1_data['course_group'].iloc[0]
                        
                        improvement_data.append({
                            'Category': category,
                            'Variable': var,
                            'Course': course,
                            'Improvement': improvement,
                            'Participant': participant
                        })
        
        if len(improvement_data) > 0:
            improvement_df = pd.DataFrame(improvement_data)
            
            # カテゴリ別改善度の箱ひげ図
            plt.figure(figsize=(12, 8))
            
            categories = ['Cognitive Skills', 'Non-Cognitive Skills']
            courses = ['eSports', 'Liberal Arts']
            
            positions = []
            data_for_boxplot = []
            labels = []
            
            pos = 1
            for category in categories:
                for course in courses:
                    cat_course_data = improvement_df[
                        (improvement_df['Category'] == category) & 
                        (improvement_df['Course'] == course)
                    ]['Improvement']
                    
                    if len(cat_course_data) > 0:
                        data_for_boxplot.append(cat_course_data)
                        positions.append(pos)
                        labels.append(f'{category}\n{course}')
                        pos += 1
                
                pos += 0.5  # カテゴリ間のスペース
            
            if len(data_for_boxplot) > 0:
                box_plot = plt.boxplot(data_for_boxplot, positions=positions, 
                                     patch_artist=True, widths=0.6)
                
                # 色分け
                colors = ['lightblue', 'lightcoral'] * len(categories)
                for patch, color in zip(box_plot['boxes'], colors):
                    patch.set_facecolor(color)
                    patch.set_alpha(0.7)
                
                plt.xticks(positions, labels, rotation=0, fontsize=12)
                plt.ylabel('Improvement (Wave 3 - Wave 1, adjusted for direction)', fontsize=14)
                plt.title('Category and Course-Specific Improvement Comparison (TMT Fixed)', fontsize=18, fontweight='bold')
                plt.grid(True, alpha=0.3)
                plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
                
                plt.tight_layout()
                save_path = os.path.join(graph_dir, "category_improvement_comparison.png")
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                plt.close()
            
    except Exception as e:
        print(f"    ⚠️ カテゴリ別グラフ: エラー - {str(e)}")

def create_correlation_heatmap(df, variables, output_dir):
    """
    変数間相関ヒートマップ（TMT単位考慮版）
    """
    
    print("  相関ヒートマップを作成中...")
    
    try:
        graph_dir = os.path.join(output_dir, "graphs")
        
        # Wave1のデータで相関計算（センタリング後）
        wave1_data = df[df['measurement_wave'] == 0]  # センタリング後: 0=Wave1
        
        # 認知スキル相関
        cognitive_corr_data = wave1_data[variables['cognitive']].corr()
        
        plt.figure(figsize=(12, 10))
        mask = np.triu(np.ones_like(cognitive_corr_data, dtype=bool))
        sns.heatmap(cognitive_corr_data, mask=mask, annot=True, cmap='coolwarm', 
                   center=0, square=True, linewidths=0.5)
        plt.title('Correlation Heatmap for Cognitive Skills (Wave 1, TMT in seconds)', fontsize=18, fontweight='bold')
        plt.tight_layout()
        
        save_path = os.path.join(graph_dir, "cognitive_correlation_heatmap.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # 非認知スキル相関
        non_cognitive_corr_data = wave1_data[variables['non_cognitive']].corr()
        
        plt.figure(figsize=(12, 10))
        mask = np.triu(np.ones_like(non_cognitive_corr_data, dtype=bool))
        sns.heatmap(non_cognitive_corr_data, mask=mask, annot=True, cmap='coolwarm',
                   center=0, square=True, linewidths=0.5)
        plt.title('Correlation Heatmap for Non-Cognitive Skills (Wave 1)', fontsize=18, fontweight='bold')
        plt.tight_layout()
        
        save_path = os.path.join(graph_dir, "non_cognitive_correlation_heatmap.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
    except Exception as e:
        print(f"    ⚠️ 相関ヒートマップ: エラー - {str(e)}")

def create_final_summary_report(summary_df, detailed_results, variables, output_dir):
    """
    最終サマリーレポートの作成（TMT単位修正版）
    """
    
    report_lines = []
    report_lines.append("="*80)
    report_lines.append("eスポーツコース効果：線形混合効果モデル（LMM）分析 最終レポート")
    report_lines.append("TMT単位修正版（ミリ秒→秒）・センタリング版（Wave1基準）")
    report_lines.append("="*80)
    report_lines.append("")
    
    # 分析概要
    report_lines.append("📊 分析概要")
    report_lines.append("-" * 40)
    report_lines.append(f"総分析変数: {len(variables['all'])}個")
    report_lines.append(f"  - 認知スキル: {len(variables['cognitive'])}個")
    report_lines.append(f"  - 非認知スキル: {len(variables['non_cognitive'])}個")
    report_lines.append(f"  - TMT完了時間: 秒単位に修正済み")
    report_lines.append(f"  - 時間変数: Wave1基準にセンタリング (0,1,2)")
    report_lines.append("")
    
    # 主要な発見
    course_sig = (summary_df['Course_P'] < 0.05).sum()
    time_sig = (summary_df['Time_P'] < 0.05).sum()
    interaction_sig = (summary_df['Interaction_P'] < 0.05).sum()
    
    report_lines.append("🎯 主要な発見")
    report_lines.append("-" * 40)
    report_lines.append(f"有意なコース効果: {course_sig}/{len(summary_df)}変数 ({course_sig/len(summary_df)*100:.1f}%)")
    report_lines.append(f"有意な時間効果: {time_sig}/{len(summary_df)}変数 ({time_sig/len(summary_df)*100:.1f}%)")
    report_lines.append(f"有意な交互作用: {interaction_sig}/{len(summary_df)}変数 ({interaction_sig/len(summary_df)*100:.1f}%)")
    report_lines.append("")
    
    # カテゴリ別分析
    report_lines.append("📋 カテゴリ別効果")
    report_lines.append("-" * 40)
    
    for category in ['cognitive', 'non_cognitive']:
        cat_data = summary_df[summary_df['Category'] == category]
        cat_course_sig = (cat_data['Course_P'] < 0.05).sum()
        cat_time_sig = (cat_data['Time_P'] < 0.05).sum()
        
        category_name = '認知スキル' if category == 'cognitive' else '非認知スキル'
        report_lines.append(f"{category_name}:")
        report_lines.append(f"  コース効果: {cat_course_sig}/{len(cat_data)}変数")
        report_lines.append(f"  時間効果: {cat_time_sig}/{len(cat_data)}変数")
        report_lines.append("")
    
    # 最も強い効果（単位情報付き）
    report_lines.append("🏆 最も強い効果を示した変数（単位修正済み）")
    report_lines.append("-" * 40)
    
    # コース効果TOP5
    top_course = summary_df.nsmallest(5, 'Course_P')
    report_lines.append("コース効果 TOP5:")
    for i, (_, row) in enumerate(top_course.iterrows(), 1):
        unit_str = f" ({row['Unit']})" if row['Unit'] else ""
        if 'tmt_combined_trailtime' in row['Variable']:
            direction = "Liberal Arts SLOWER" if row['Course_Coef'] > 0 else "eSports SLOWER"
        elif 'rt' in row['Variable']:
            direction = "Liberal Arts SLOWER" if row['Course_Coef'] > 0 else "eSports SLOWER"
        elif 'errors' in row['Variable']:
            direction = "Liberal Arts MORE errors" if row['Course_Coef'] > 0 else "eSports MORE errors"
        else:
            direction = "Liberal Arts > eSports" if row['Course_Coef'] > 0 else "eSports > Liberal Arts"
        report_lines.append(f"  {i}. {row['Variable']}{unit_str} (p={row['Course_P']:.4f}) [{direction}]")
    report_lines.append("")
    
    # 時間効果TOP5
    top_time = summary_df.nsmallest(5, 'Time_P')
    report_lines.append("時間効果 TOP5:")
    for i, (_, row) in enumerate(top_time.iterrows(), 1):
        unit_str = f" ({row['Unit']})" if row['Unit'] else ""
        if 'tmt_combined_trailtime' in row['Variable'] or 'rt' in row['Variable'] or 'errors' in row['Variable']:
            direction = "improvement (decrease)" if row['Time_Coef'] < 0 else "deterioration (increase)"
        else:
            direction = "improvement (increase)" if row['Time_Coef'] > 0 else "deterioration (decrease)"
        report_lines.append(f"  {i}. {row['Variable']}{unit_str} (p={row['Time_P']:.4f}) [{direction}]")
    report_lines.append("")
    
    # TMT特別解説
    tmt_data = summary_df[summary_df['Variable'].str.contains('tmt', case=False)]
    if len(tmt_data) > 0:
        report_lines.append("🎮 TMT課題の特別解説")
        report_lines.append("-" * 40)
        report_lines.append("TMT（Trail Making Test）完了時間は秒単位に修正済み")
        report_lines.append("時間が短いほど良いパフォーマンスを示します")
        
        for _, row in tmt_data.iterrows():
            if row['Course_P'] < 0.05:
                if row['Course_Coef'] > 0:
                    course_interpretation = f"Liberal Artsの方が{abs(row['Course_Coef']):.2f}秒遅い（eSportsが有利）"
                else:
                    course_interpretation = f"eSportsの方が{abs(row['Course_Coef']):.2f}秒遅い（Liberal Artsが有利）"
                report_lines.append(f"  {row['Variable']}: {course_interpretation} (p={row['Course_P']:.4f})")
        report_lines.append("")
    
    # レポート保存（テキストとExcel両方）
    try:
        # テキストファイル
        text_path = os.path.join(output_dir, 'final_lmm_report.txt')
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        print(f"📄 最終レポートを保存: {text_path}")
        
        # Excelレポート
        excel_report_path = os.path.join(output_dir, 'lmm_analysis_report.xlsx')
        with pd.ExcelWriter(excel_report_path, engine='openpyxl') as writer:
            # サマリー統計
            summary_stats = pd.DataFrame({
                '項目': ['総変数数', '認知スキル変数', '非認知スキル変数', 
                        '有意なコース効果', '有意な時間効果', '有意な交互作用'],
                '数値': [len(variables['all']), len(variables['cognitive']), len(variables['non_cognitive']),
                        course_sig, time_sig, interaction_sig]
            })
            summary_stats.to_excel(writer, sheet_name='分析サマリー', index=False)
            
            # TOP効果
            top_course.to_excel(writer, sheet_name='コース効果TOP5', index=False)
            top_time.to_excel(writer, sheet_name='時間効果TOP5', index=False)
            
            # TMT特別シート
            if len(tmt_data) > 0:
                tmt_data.to_excel(writer, sheet_name='TMT課題結果', index=False)
        
        print(f"📊 Excelレポートを保存: {excel_report_path}")
        
    except Exception as e:
        print(f"⚠️ レポート保存エラー: {str(e)}")
    
    # コンソールにも表示
    print('\n'.join(report_lines))

def main():
    """メイン実行関数（TMT単位修正版・センタリング版）"""
    
    print("🔧 線形混合効果モデル（LMM）分析開始 - 全25変数 (TMT単位修正版・センタリング版)")
    print("="*80)
    
    # 出力ディレクトリの作成
    output_dir = setup_output_directory()
    
    # データ読み込み
    df = load_preprocessed_data()
    if df is None:
        return
    
    # TMT単位変換
    df, converted_vars = convert_tmt_units(df)
    
    # 分析変数定義
    variables = define_analysis_variables()
    
    print(f"\n📋 分析対象:")
    print(f"  認知スキル変数: {len(variables['cognitive'])}個")
    print(f"  非認知スキル変数: {len(variables['non_cognitive'])}個")
    print(f"  総分析変数: {len(variables['all'])}個")
    print(f"  TMT変換済み変数: {converted_vars}")
    print(f"  出力先: {output_dir}")
    
    # Phase 1: 全変数のLMM分析
    print(f"\n🎯 Phase 1: 全25変数のLMM分析 (TMT単位修正版)")
    print("-" * 60)
    
    summary_df = create_comprehensive_lmm_summary(df, variables, output_dir)
    
    # Phase 2: 有意な効果のあった変数の詳細分析
    print(f"\n�� Phase 2: 有意な効果のあった変数の詳細分析")
    print("-" * 60)
    
    detailed_results = run_detailed_analysis_for_significant_vars(df, summary_df)
    
    # Phase 3: 可視化（インタラクティブ + 静的）
    print(f"\n📊 Phase 3: 可視化作成 (TMT単位考慮)")
    print("-" * 60)
    
    # 最も強い効果のあった変数を可視化（インタラクティブ軌跡図）
    top_vars = summary_df.nsmallest(5, 'Course_P')['Variable'].tolist()
    top_vars.extend(summary_df.nsmallest(3, 'Time_P')['Variable'].tolist())
    top_vars = list(set(top_vars))  # 重複除去
    
    print(f"インタラクティブ軌跡図対象: {top_vars}")
    
    # インタラクティブ軌跡図
    for var in top_vars:
        if var in df.columns:
            try:
                visualize_individual_trajectories(df, var, output_dir)
            except Exception as e:
                print(f"⚠️ {var}の軌跡図でエラー: {str(e)}")
    
    # 静的グラフ作成（全変数対象）
    print(f"静的グラフ対象: 全{len(variables['all'])}変数")
    create_static_visualizations(df, variables, output_dir)
    
    # 相関ヒートマップ
    create_correlation_heatmap(df, variables, output_dir)
    
    # Phase 4: 総合サマリー
    print(f"\n📈 Phase 4: 総合分析サマリー")
    print("-" * 60)
    
    create_final_summary_report(summary_df, detailed_results, variables, output_dir)
    
    print(f"\n✅ 全25変数のLMM分析完了!")
    print(f"📁 以下のファイルが生成されました:")
    print(f"   📊 {output_dir}/lmm_results_comprehensive.xlsx (全結果)")
    print(f"   📈 {output_dir}/lmm_analysis_report.xlsx (分析レポート)")
    print(f"   📄 {output_dir}/final_lmm_report.txt (テキストレポート)")
    print(f"   🌐 {output_dir}/trajectory_*.html (個人軌跡図)")
    print(f"   📉 {output_dir}/graphs/ (静的グラフ集)")
    print(f"      - group_mean_*.png (群平均比較)")
    print(f"      - effect_sizes_comparison.png (効果サイズ)")
    print(f"      - category_improvement_comparison.png (カテゴリ別改善)")
    print(f"      - *_correlation_heatmap.png (相関ヒートマップ)")
    print(f"\n🔧 修正内容:")
    print(f"   - TMT完了時間: ミリ秒 → 秒単位に変換")
    print(f"   - 時間変数: Wave1基準にセンタリング (1,2,3 → 0,1,2)")
    print(f"   - 効果サイズ計算: TMT時間は短い方が良いパフォーマンスとして調整")
    print(f"   - 結果解釈: 切片はWave1時点の値、傾きは1回の測定ごとの変化量")
    print(f"   - 可視化: Y軸ラベルに単位情報を追加")
    
    return {
        'summary_df': summary_df,
        'detailed_results': detailed_results,
        'variables': variables,
        'converted_vars': converted_vars,
        'output_dir': output_dir
    }

if __name__ == "__main__":
    results = main()