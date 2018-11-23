# Fraud_detection
click fraud detection


python main.py --walks /data/zy/youmi/FDMA/metapath/train/random_walk.txt --types /data/zy/youmi/FDMA/metapath/train/node_type_mapings.txt --window 7 --lr 0.02 --negative-samples 5 --embedding-dim 128 --batch 10000 --opt-algo 'adam' --care-type 0 --log /data/zy/youmi/FDMA/metapath/train/log
