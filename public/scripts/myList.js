function prepareDictData (data_dict) 
{
        return Object.entries(data_dict)
}

function createList  (data, parent_id, prepare_data_func=prepareDictData) 
{
        data = prepare_data_func(data)
        console.log('createList: data=', data)
        var ul = d3.select(parent_id).append ('ul');
        ul.selectAll('li')
          .data  (data).enter()
          .append('li')
          .html  (function(d) {
                     if (d[0]=='User') 
                        return '<span>' + d[0] + ':</span><a href=./userDetails?user=' + d[1]+'>' + d[1] + '</a>'
                     return '<span>' + d[0] + ':</span>' + d[1]})
};

