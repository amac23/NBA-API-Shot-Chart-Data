import matplotlib.pyplot as plt
import pandas as pd

shot_df = pd.read_csv('./assets/data/shots.csv')

plt.scatter(shot_df.LOC_X, shot_df.LOC_Y)
plt.savefig('./assets/images/test_shot_chart.png')
