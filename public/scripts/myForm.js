//q3 data binding need to arrays
function prepareDictData (data_dict, fields_dict) 
{
    if (!fields_dict)
       return Object.entries(data_dict)
    fields = Object.keys(fields_dict)
    f = fields.filter(function (k) {return data_dict.hasOwnProperty(k)})  //keep the order in fields
               // is different from the same named function in myList
    console.log("---", f)
    return f.map(function (k) { return [fields_dict[k], data_dict[k], k] })
}

//data, fields, type_dict are all objects(python dict)
//fields are field:field_display_title
function createFormInput  (setting_key, dataObj, fields, typeObj, parent_id, prepare_data_func=prepareDictData) 
{
        var data   = prepare_data_func(dataObj, fields)
        var form   = d3.select(parent_id);
        var divs   = form.selectAll("span").data(data).enter().append("span")
        var checkbox_flds = []
        console.log('createFormInput: data=', data, ',fields=', fields, ",type_dict=")
        divs.append('input')
          .attr  ('type',  function(d) { var type=typeObj[d[2]]; if (type == 'percent') {return "number"} else {return type}})
          .attr  ('step',  function(d) { return typeObj[d[2]]=='percent'? 0.01:1})
          .attr  ('min',   0)
          .attr  ('max',   function(d) { return typeObj[d[2]]=='percent'? 99.99:999})
          .attr  ('value', function(d) { if (typeObj[d[2]]=="checkbox") {checkbox_flds.push(d[2]); return true;} else return d[1];})
          .attr  ('name',  function(d) { return d[2];})
          .attr  ('id',    function(d) { return d[2];})
          .property  ('checked', function(d) {return d[1];})
        divs.append('label')
          .text  (function(d) {return d[0]})
      
        form.append('input')   // hidden field to return the key
          .attr  ('type',  "hidden")
          .attr  ('name',  "setting_key")
          .attr  ('value', setting_key)
        form.append('input')   // hidden field to return unchecked email 
          .attr  ('type',  "hidden")
          .attr  ('name',  "checkbox")
          .attr  ('value', checkbox_flds)
        form.append('input')
          .attr  ('type',  "submit")
          .attr  ('value', "Submit")
          .attr  ('style', "float:right; color:#3A509C")
};
