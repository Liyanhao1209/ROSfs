mkdir ./AoI/"$1"

mkdir /data/GroundAir/dataset/MC/"$1"/aerial
mkdir /data/GroundAir/dataset/MC/"$1"/aerial/image
mkdir /data/GroundAir/dataset/MC/"$1"/aerial/pose
mkdir /data/GroundAir/dataset/MC/"$1"/street
mkdir /data/GroundAir/dataset/MC/"$1"/street/image
mkdir /data/GroundAir/dataset/MC/"$1"/street/pose

mv /data/GroundAir/dataset/MC/"$1"/split_x00_y00/image/a* /data/GroundAir/dataset/MC/"$1"/aerial/image
mv /data/GroundAir/dataset/MC/"$1"/split_x00_y00/image/s* /data/GroundAir/dataset/MC/"$1"/street/image
mv /data/GroundAir/dataset/MC/"$1"/split_x00_y00/pose/a* /data/GroundAir/dataset/MC/"$1"/aerial/pose
mv /data/GroundAir/dataset/MC/"$1"/split_x00_y00/pose/s* /data/GroundAir/dataset/MC/"$1"/street/pose

rm -r /data/GroundAir/dataset/MC/"$1"/split_x00_y00

source /data/venv/bin/activate

python3 BagSplit.py --dataset /data/GroundAir/dataset/MC/"$1" --output ./split-1 --ttime 3600 --partition 1
python3 Monitor.py --bag ./split-1 --ttime 3600 --partition 1
mv ./split-1/1.aoi ./AoI/"$1"
rm -r ./split-1

python3 BagSplit.py --dataset /data/GroundAir/dataset/MC/"$1" --output ./split-4 --ttime 3600 --partition 4
python3 Monitor.py --bag ./split-4 --ttime 3600 --partition 4
mv ./split-4/4.aoi ./AoI/"$1"
rm -r ./split-4

python3 BagSplit.py --dataset /data/GroundAir/dataset/MC/"$1" --output ./split-16 --ttime 3600 --partition 16
python3 Monitor.py --bag ./split-16 --ttime 3600 --partition 16
mv ./split-16/16.aoi ./AoI/"$1"
rm -r ./split-16

rm -r /data/GroundAir/dataset/MC/"$1"