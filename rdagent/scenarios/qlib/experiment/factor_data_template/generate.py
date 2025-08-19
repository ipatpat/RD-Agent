import qlib

qlib.init(provider_uri="~/.qlib/qlib_data/cn_data")

from qlib.data import D

instruments = D.instruments()
fields = ["$open", "$close", "$high", "$low", "$volume", "$factor"]
data = D.features(instruments, fields, freq="day").swaplevel().sort_index().loc["2008-12-29":].sort_index()

data.to_hdf("./daily_pv_all.h5", key="data")

features = (
    D.features(instruments, fields, start_time="2018-01-01", end_time="2019-12-31", freq="day")
    .swaplevel()
    .sort_index()
)

# 在目标时间段内实际有数据的 instruments（基于 features）
available_instruments = features.reset_index()["instrument"].unique()
n_available = len(available_instruments)
print(f"Found {n_available} instruments with data in the debug period.")

# 选择样本（默认使用前 100 个可用的 instruments；若想使用全部则去掉切片）
selected_instruments = available_instruments[:100]
if n_available < 100:
    print(f"Note: only {n_available} instruments available; using all of them.")

# 安全切片：先 swaplevel，使 instrument 可作为索引层来筛选
data = features.swaplevel().loc[selected_instruments].swaplevel().sort_index()
data.to_hdf("./daily_pv_debug.h5", key="data")
print("Wrote daily_pv_all.h5 and daily_pv_debug.h5")