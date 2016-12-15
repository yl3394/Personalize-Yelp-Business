// The base endpoint to receive data from. See update_url()
var URL_BASE = "http://ec2-52-201-219-76.compute-1.amazonaws.com/";
var BIZ_SEPARATOR = '|';


function getBusinessNames(name) {
    var xmlHttp = new XMLHttpRequest();

    theUrl = URL_BASE + 'business/get_name/' + name;

    xmlHttp.onreadystatechange = function() { 
        if (xmlHttp.readyState == 4 && xmlHttp.status == 200)
	    createTable("bizOutput", xmlHttp.responseText);
    }
    xmlHttp.open("GET", theUrl, true); // true for asynchronous 
    xmlHttp.send(null);
}

function createCol(tr, title) {
    var td = tr.insertCell();
    td.appendChild(document.createTextNode(title));
    td.style.border = '1px solid black';
    td.innerHTML = title;
}

function createTable(elemId, response) {
    elem = document.getElementById(elemId);

    // clear table
    while (elem.firstChild)
	elem.removeChild(elem.firstChild);

    arr = response.split(BIZ_SEPARATOR);
    if (arr.length == 0 || arr[0].length == 0) {
	elem.innerHTML = "No results found";
	return;
    }

    var tbl = document.createElement('table');
    tbl.style.width  = '100px';
    tbl.style.border = '1px solid black';

    // create headers
    var tr = tbl.insertRow();
    createCol(tr, 'Name');
    createCol(tr, 'City');
    createCol(tr, 'State');
    createCol(tr, 'Avg. Rating');
    createCol(tr, 'Num. Reviews');
    createCol(tr, 'Categories');

    for (var i = 0; i < arr.length; ++i) {
	tmp = arr[i].split(';');
        bizName = tmp[0];
        bizID =   tmp[1];	

	var tr = tbl.insertRow();

	// create hyperlink with restaurant ID
        chartURL = URL_BASE + 'business/build_chart/' + bizID;
        urlHtml = '<a href="' + chartURL + '" target="_blank">' + bizName + '</a>';
	createCol(tr, urlHtml);

	createCol(tr, tmp[2]);  // city
	createCol(tr, tmp[3]);	// state
	createCol(tr, tmp[4]);  // stars
	createCol(tr, tmp[5]);  // review count
	createCol(tr, tmp[6]);  // categories
    }

    elem.appendChild(tbl);
}
