
all:	kafkafs

kafkafs:	kafkafs.c
	gcc -A -Wall kafkafs.c \
		-DPACKAGE_VERSION=\"1.0.0\" \
		-lrdkafka -lz -lpthread -lrt \
		`pkg-config fuse --cflags --libs` \
		-o kafkafs

clean: 
	rm kafkafs


