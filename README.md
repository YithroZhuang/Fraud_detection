# Fraud_detection
click fraud detection


# run generateMetaPath
## argv1 0 for bipartite graph and 1 for tripartite graph
## 80 random walks and 50 walk length
python generate_data.py 0 /data/zy/youmi/FDMA/original_data/clicks_09feb12.csv /data/zy/youmi/FDMA/metapath/train/ 80 50



# run metapath2vec
python main.py --walks /data/zy/youmi/FDMA/metapath/train/random_walk.txt --types /data/zy/youmi/FDMA/metapath/train/node_type_mapings.txt --window 7 --lr 0.02 --negative-samples 5 --embedding-dim 128 --batch 10000 --opt-algo 'adam' --care-type 0 --log /data/zy/youmi/FDMA/metapath/train/log
