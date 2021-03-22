import numpy as np
import pandas as pd


def get_FPD(X):
    counts = X.value_counts()
    total = counts.sum()

    def FPD(y):
        if y in counts:
            return counts[y]/total
        return 0
    return FPD


def FPD_to_array(f, X):
    return np.array([f(x) for x in range(int(X.max()))])


def Bhattacharyya(A, B):
    # Assumes A.len() == B.len()
    # Note: We don't include indices as in the paper on FPD-LOF.
    return - np.log(np.sum(np.sqrt(A*B)))


def adjust_len(A, s):
    return np.pad(A, (0, s-len(A)), 'constant')


def df_to_FPD_vectors(df):
    cols = df.columns
    FPDs = {col: FPD_to_array(get_FPD(df[col]), df[col]) for col in cols}
    max_len = max([len(x) for x in FPDs.values()])
    return pd.DataFrame({col: adjust_len(x, max_len) for col, x in FPDs.items()})


def df_to_FPD_dist_matrix(df):
    df = df_to_FPD_vectors(df).corr(Bhattacharyya)
    np.fill_diagonal(df.values, 0)
    return df


def get_kNN(df, k):
    return pd.DataFrame({col: df[col].sort_values().index[:k] for col in df.columns})


def FPD_LOF(df, k):
    dists = df_to_FPD_dist_matrix(df)
    kNN = get_kNN(dists, k)
    def reach(a, b): return max(dists[b][kNN[b].iloc[-1]], dists[a][b])
    def lrd(f): return 1/(sum([reach(f, i) for i in kNN[f]])/k)
    def LOF(f): return (1/k) * sum([lrd(f)/lrd(i) for i in kNN[f]])
    return pd.Series({col: LOF(col) for col in df.columns})
