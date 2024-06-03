mf=$(find . -name Makefile)
dotmf=$(find . -name '*.make*)
for i in $mf $dotmf; do
    cat $i | sed \
	-e "s/transforms.python-publish/transforms.publish-image-python/g" 	\
	-e "s/transforms.ray-publish/transforms.publish-image-ray/g" 		\
	-e "s/transforms.spark-publish/transforms.publish-image-spark/g" 	\
	> tt
    mv tt $i
done
	
