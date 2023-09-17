import collections
import polars as pl

original_df = pl.read_excel('/Users/wei/Documents/cara_network/amr_igdata/output/original_instagram_data.xlsx')
error_df2 = pl.read_csv('/Users/wei/Documents/cara_network/amr_igdata/error_out_test.csv')
post_pro_df3 = pl.read_csv('/Users/wei/Documents/cara_network/amr_igdata/output/n_images_video_with_id.csv')
merge_df = post_pro_df3.join(error_df2, on='id', how='outer')
merge_df = original_df.join(merge_df, on='id')
merge_df = original_df.join(post_pro_df3, on='id')
# merge_df.write_csv('/Users/wei/Documents/cara_network/amr_igdata/merge_test.csv')

ret = collections.defaultdict(list)
label_df = pl.read_csv('/Users/wei/code/rscvp/src/rscvp/util/image/test2.csv')
id = label_df['filename']
note = label_df['notes']
for ids in id:
    ret['id'].append(ids.split('_')[1])
    ret['id_'].append(ids.split('_')[2])
for notes in note:
    ret['notes'].append(notes)

df = pl.DataFrame(ret)

label_df = df.pivot(index='id', columns='id_', values='notes')
label_df = label_df.with_columns(pl.col('id').cast(pl.Int64))

final_df = merge_df.join(label_df, on='id')

final_df.write_excel('/Users/wei/Documents/cara_network/amr_igdata/final_dftest.xlsx')
