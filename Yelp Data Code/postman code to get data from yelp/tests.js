const offsets = pm.collectionVariables.get("offsets");

if (offsets && offsets.length > 0){
    postman.setNextRequest("Loop Query Parameter");
} else {
    postman.setNextRequest(null);
}
pm.test("Status code is 200", function () {
    pm.response.to.have.status(200);
});

let responses = pm.globals.has('responses') ?  JSON.parse(pm.globals.get('responses')) : [];
responses.push(pm.response.json());
pm.globals.set('responses', JSON.stringify(responses));