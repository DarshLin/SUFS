//This is just a test for instantiating a new ec2 instance from code.
//If successful, this can be used for fault tolerance
//The only thing it would need is the proper variables (AMI ID, Launch Template ID
//and execution upon a failure of a data node.

//@@@Please Refer to Instructions below the code for creating launch templates


var createDN = {}
createDN.create = createNewDataNode;

function createNewDataNode() {


    var AWS = require("aws-sdk");

    AWS.config.update({
        accessKeyId: 'Access Key ID',
        secretAccessKey: 'Secret Access Key',
        region: 'Region'
    });

    var ec2 = new AWS.EC2({ apiVersion: '2016-11-15' });
    var ec2Params = {
        MaxCount: 1,
        MinCount: 1,
        // DryRun: true, //This is only to test construction of EC2 instance
        ImageId: 'ami-5', //***This needs to be changed with new AMI (replace with AMI ID)
        InstanceType: 't2.micro',
        LaunchTemplate: {
            LaunchTemplateId: 'lt-5', //***This needs to be changed with new launch template (Launch Template ID)
            Version: '5'
        },
        SecurityGroupIds: [
            'sg-1',
            'sg-2',
            'sg-3',
        ],
    };

    ec2.runInstances(ec2Params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data); // successful response
    });
}

// createNewDataNode();

module.exports = createDN;

/*@@@LAUNCH TEMPLATE INSTRUCTIONS
    1) Make a name
    2) Create a small description to help identify
    3) Put in the target AMI ID
    4) Select Instance Type: t2.micro
    5) Optional: only if you want access to it through SSH
        Put in a Key Pair
    6) VPC Selected; Not Classic
    7) DON'T add any Network Interfaces or Storage(Volumes)
    8) DO add new tag | Key: Name | Value: Whatever you want to call new instances
        Tag Instances and Tag Volumes checked
    9) DO add a total of 3 security groups, The IDs are above in 
        'SecurityGroupIds' alternatively, grab them from 'Security Groups' in
        EC2 Dashboard.
    10) DON'T bother with Advanced Details
     
     Edit 'ImageId' and 'LaunchTemplateId' above and run the function,
     it should produce a new EC2 instance from the AMI you made of the 
     Data Node.

*/


//runInstances(params = {}, callback) ⇒ AWS.Request
//This may be what launches instances from code.

//assignPrivateIpAddresses(params = {}, callback) ⇒ AWS.Request //This may be useful so we don't 
//have to change IPs when constructing new datanode.

//bundleInstance(params = {}, callback) ⇒ AWS.Request
//Not sure if this needed for AMI instance

//createLaunchTemplate(params = {}, callback) ⇒ AWS.Request
//Might be a launch template rather than an AMI

//https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/EC2.html
//This is the API for anything EC2, lots of methods for fault tolerance
