let offsets = pm.collectionVariables.get("offsets");

var offset_temp = []
for(let i=0;i<1000;i=i+50)
{
    offset_temp.push(i)
}

if(!offsets || offsets.length == 0) {
    offsets = offset_temp;
}
let currentOffset = offsets.shift();
console.log('Current Offset: '+currentOffset)
pm.collectionVariables.set("offset", currentOffset);
pm.collectionVariables.set("offsets", offsets);