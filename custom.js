//initializing the constants

channel_select_cnt = 1;
let og_range1;
let og_range2;
let chart_data;
let chart_layout;

const channel_mapping = {
  1: "LS1Q1D REF",
  2: "LS1Q1D RDBK",
  3: "LS2Q1D REF",
  4: "LS2Q1D RDBK",
  5: "LS3Q1D REF",
  6: "LS3Q1D RDBK",
  7: "LS4Q1D REF",
  8: "LS4Q1D RDBK",
  9: "LS5Q1D REF",
  10: "LS5Q1D RDBK",
  11: "LS6Q1D REF",
  12: "LS6Q1D RDBK",
  13: "LS7Q1D REF",
  14: "LS7Q1D RDBK",
  15: "LS8Q1D REF",
  16: "LS8Q1D RDBK",
  17: "LS1Q2F REF",
  18: "LS1Q2F RDBK",
  19: "LS2Q2F REF",
  20: "LS2Q2F RDBK",
  21: "LS3Q2F REF",
  22: "LS3Q2F RDBK",
  23: "LS4Q2F REF",
  24: "LS4Q2F RDBK",
  25: "LS5Q2F REF",
  26: "LS5Q2F RDBK",
  27: "LS6Q2F REF",
  28: "LS6Q2F RDBK",
  29: "LS7Q2F REF",
  30: "LS7Q2F RDBK",
  31: "LS8Q2F REF",
  32: "LS8Q2F RDBK",
  33: "LS1Q3D REF",
  34: "LS1Q3D RDBK",
  35: "LS2Q3D REF",
  36: "LS2Q3D RDBK",
  37: "LS3Q3D REF",
  38: "LS3Q3D RDBK",
  39: "LS4Q3D REF",
  40: "LS4Q3D RDBK",
  41: "LS5Q3D REF",
  42: "LS5Q3D RDBK",
  43: "LS6Q3D REF",
  44: "LS6Q3D RDBK",
  45: "LS7Q3D REF",
  46: "LS7Q3D RDBK",
  47: "LS8Q3D REF",
  48: "LS8Q3D RDBK",
  49: "SSQ4 REF",
  50: "SSQ4 RDBK",
  51: "DP REF",
  52: "DP RDBK",
  53: "SSQ5 REF",
  54: "SSQ5 RDBK",
  55: "SPR1",
  56: "SPR2",
  57: "SSSF REF",
  58: "SSSF RDBK",
  59: "SSSD REF",
  60: "SSSD RDBK",
  61: "SPR3",
  62: "SPR4",
  63: "SPR5",
  64: "SPR6",
  65: "RF1 REF",
  66: "RF1 RDBK",
  67: "RF2 REF",
  68: "RF2 RDBK",
  69: "RF3 REF",
  70: "RF3 RDBK",
  71: "RF4 REF",
  72: "RF4 RDBK",
  73: "RF5 REF",
  74: "RF5 RDBK",
  75: "RF6 REF",
  76: "RF6 RDBK",
  77: "DCCT",
  78: "SPR7",
  79: "SPR8",
  80: "SPR9",
};

const selected_filters = {
  Event_Type: "Beam Kill",
  Event_Date: undefined,
  Event_File: undefined,
  channel_list: ["1", "77"],
};

config = {
  onDownloadProgress: (ProgressEvent) => {
    console.log(ProgressEvent);
  },
  headers: {
    "Content-type": "application/json; charset=UTF-8",
  },
};

//end of constant declaration

//html element creation functions

//this function creates the names of channel in green bubble in the filter panel
function channel_list_small_div_creator(channel_name) {
  cno_div = document.createElement("div");
  cno_div.setAttribute("class", "channel_list_in_filter");
  cno_div.innerHTML = `${channel_name}`;
  return cno_div;
}

//this function creates the spinner used in loading plots
let create_spinner = function (type, id) {
  div_el = document.createElement("div");
  div_el.setAttribute("class", `spinner-border text-${type}`);
  div_el.setAttribute("role", "status");
  div_el.setAttribute("style", "width: 5rem; height: 5rem;");
  div_el.setAttribute("id", id);
  span_el = document.createElement("span");
  span_el.setAttribute("class", "sr-only");
  div_el.appendChild(span_el);
  return div_el;
};

//this function creates the divs that show the metrics like mean, rms etc.
function metrics_div_maker(channel_no) {
  let main_div = document.getElementById("derived-metrics");
  let metric_cont = document.createElement("div");
  metric_cont.setAttribute("id", "cont-1");
  metric_cont.setAttribute("class", "metrics-cont");
  let metric_h5 = document.createElement("h5");
  metric_h5.innerHTML = `${channel_mapping[channel_no]} Metrics`;
  metric_cont.appendChild(metric_h5);
  main_div.appendChild(metric_cont);
  let content_cont = document.createElement("div");
  content_cont.setAttribute("class", "content-cont");
  content_cont.setAttribute("id", `content-cont-${channel_no}`);
  content_cont.appendChild(create_spinner("primary", "ps-1"));
  metric_cont.appendChild(content_cont);
}

//this functions inserts the values of the metrics in form of a list in the above created div
function create_list_of_metrics(res_data, channel_no) {
  let content_cont = document.getElementById(`content-cont-${channel_no}`);
  content_cont.replaceChildren();
  let ul_el = document.createElement("ul");
  ul_el.setAttribute("class", "metric-list");
  let li_el_1 = document.createElement("li");
  let li_el_2 = document.createElement("li");
  let li_el_3 = document.createElement("li");
  let li_el_4 = document.createElement("li");
  let li_el_5 = document.createElement("li");
  let li_el_6 = document.createElement("li");
  let li_el_7 = document.createElement("li");
  li_el_1.innerHTML = `Arithmetic Mean : ${res_data["mean_value"]}`;
  ul_el.appendChild(li_el_1);
  li_el_2.innerHTML = `RMS : ${res_data["mean_squared_value"]}`;
  ul_el.appendChild(li_el_2);
  li_el_3.innerHTML = `Standard Dev : ${res_data["std_value"]}`;
  ul_el.appendChild(li_el_3);
  li_el_4.innerHTML = `Maximum : ${res_data["max_val"]}`;
  ul_el.appendChild(li_el_4);
  li_el_5.innerHTML = `Time of Maximum : ${res_data["max_val_time"]}`;
  ul_el.appendChild(li_el_5);
  li_el_6.innerHTML = `Minimum : ${res_data["min_val"]}`;
  ul_el.appendChild(li_el_6);
  li_el_7.innerHTML = `Time of Minimum : ${res_data["min_val_time"]}`;
  ul_el.appendChild(li_el_7);
  content_cont.appendChild(ul_el);
}

function create_channel_checkbox() {
  div_element = document.getElementById("f3-div");
  div_element.replaceChildren();
  for (j = 0; j < 80; j++) {
    button_element = document.createElement("button");
    button_element.id = `f3 - opt - ${j + 1} `;
    button_element.setAttribute(
      "class",
      "dropdown-item text-right filter-3-opt"
    );
    button_element.type = "button";
    label_element = document.createElement("label");
    label_element.innerHTML = `${channel_mapping[j + 1]} `;
    label_element.for = `f3 - opt - ${j + 1} -chkbox`;
    input_element = document.createElement("input");
    input_element.id = `f3 - opt - ${j + 1} -chkbox`;
    input_element.setAttribute("class", "f3-chkbox");
    input_element.type = "checkbox";
    input_element.value = j + 1;
    console.log(
      "array : ",
      selected_filters["channel_list"],
      "element",
      input_element.value
    );
    if (selected_filters["channel_list"].includes(input_element.value)) {
      input_element.checked = true;
    }
    button_element.appendChild(label_element);
    button_element.appendChild(input_element);
    div_element.appendChild(button_element);
  }

  for (box = 0; box < 80; box++) {
    selected_chk_box = document.getElementById(`f3 - opt - ${box + 1} -chkbox`);
    selected_chk_box.addEventListener("input", function () {
      if (this.checked) {
        selected_filters["channel_list"].push(this.value);
        channel_select_cnt++;
        set_filter_4(selected_filters["channel_list"]);
        if (channel_select_cnt == 3) {
          for (z = 0; z < 80; z++) {
            selected_chk_box = document.getElementById(
              `f3 - opt - ${z + 1} -chkbox`
            );
            if (!selected_chk_box.checked) selected_chk_box.disabled = true;
          }
        }
      } else {
        index = selected_filters["channel_list"].indexOf(this.value);
        if (index > -1) {
          selected_filters["channel_list"].splice(index, 1);
          channel_select_cnt--;
          set_filter_4(selected_filters["channel_list"]);
          if (channel_select_cnt < 3) {
            for (z = 0; z < 80; z++) {
              selected_chk_box = document.getElementById(
                `f3 - opt - ${z + 1} -chkbox`
              );
              if (!selected_chk_box.checked) selected_chk_box.disabled = false;
            }
          }
        }
      }
    });
  }
}

//end of html element creation function declaration

//filter panel setting function

function set_calendar(date) {
  document.getElementById("calendar").value = date;
  set_filter_2(date);
}

function set_filter_1(f1) {
  document.getElementById("fbnr-list-li-1").innerHTML = `Event Type : ${f1}`;
  selected_filters["Event_Type"] = f1;
}

function set_filter_2(f2) {
  document.getElementById("fbnr-list-li-2").innerHTML = `Event Date : ${f2}`;
  selected_filters["Event_Date"] = f2;
}

function set_filter_3(f3) {
  document.getElementById("fbnr-list-li-3").innerHTML = `Event File : ${f3}`;
  selected_filters["Event_File"] = f3;
}

function set_filter_4(f4) {
  channels = "";
  document.getElementById("channel-list-div").replaceChildren();
  for (temp = 0; temp < f4.length; temp++)
    document
      .getElementById("channel-list-div")
      .appendChild(channel_list_small_div_creator(channel_mapping[f4[temp]]));
  selected_filters["channel_list"] = f4;
}

//end of filter panel functions

//the functions making network calls

//function to load the file names in the event file filter dropdown
function load_available_files(selected_filters) {
  //creation of the spinner in the event file filter
  next_filter_btn = document.getElementById("f2-btn");
  spinner_el = document.createElement("span");
  spinner_el.setAttribute("class", "spinner-border spinner-border-sm");
  spinner_el.role = "status";
  spinner_el.id = "f2-spinner";
  spinner_el.setAttribute("aria-hidden", "true");
  next_filter_btn.appendChild(spinner_el);

  axios.post("/event_file", selected_filters, config).then((response) => {
    div_element = document.getElementById("f2-div");
    div_element.replaceChildren();
    for (j = 0; j < response.data["file_names"].length; j++) {
      button_element = document.createElement("button");
      button_element.id = `f2 - opt - ${j + 1} `;
      button_element.setAttribute(
        "class",
        "dropdown-item text-right filter-2-opt"
      );
      button_element.type = "button";
      button_element.innerHTML = response.data["file_names"][j];
      div_element.appendChild(button_element);
    }
    next_filter_btn.removeChild(spinner_el);

    //set event listner of the file list
    let filter2btn = document.getElementsByClassName("filter-2-opt");
    for (i = 0; i < filter2btn.length; i++) {
      filter2btn[i].addEventListener("click", function () {
        set_filter_3(this.innerHTML);
        //create_plot_function(selected_filters);
      });
    }
  });
}

//create plot function
function create_plot_function(selected_filters) {
  document.getElementById("zoom").value = "100";
  let plot_cnt_div = document.getElementById("plot-container");
  plot_cnt_div.replaceChildren();
  plot_cnt_div.appendChild(create_spinner("primary", "ps-1"));
  document.getElementById("derived-metrics").replaceChildren();

  const chk_box_val = [];
  let inputChkBox = document.getElementsByClassName("f3-chkbox");
  for (k = 0; k < inputChkBox.length; k++) {
    if (inputChkBox[k].checked) {
      chk_box_val.push(inputChkBox[k].value);
    }
  }
  selected_filters["channel_list"] = chk_box_val;
  //alert(JSON.stringify(selected_filters));

  axios.post("/get_plot", selected_filters, config).then((response) => {
    let resp_data = response.data;
    console.log(resp_data)
    let x = resp_data["arr_x"];
    let data = [];

    for (chval = 0; chval < selected_filters.channel_list.length; chval++) {
      if (selected_filters.channel_list[chval] !== "77") {
        dummy_data_obj = {
          x: x,
          type: "scatter",
        };
        dummy_data_obj["y"] =
          resp_data[`Channel_${selected_filters.channel_list[chval]}`];
        dummy_data_obj["name"] = `${
          channel_mapping[selected_filters.channel_list[chval]]
        }`;
        data.push(dummy_data_obj);
      } else {
        dummy_data_obj = {
          x: x,
          type: "scatter",
          yaxis: "y2",
        };
        dummy_data_obj["y"] =
          resp_data[`Channel_${selected_filters.channel_list[chval]}`];
        dummy_data_obj["name"] = `${
          channel_mapping[selected_filters.channel_list[chval]]
        }`;
        data.push(dummy_data_obj);
      }
    }
    let layout = {
      autosize: false,
      width: 1300,
      height: 600,
      xaxis: {
        tickmode: "linear",
        tick0: "0",
        dtick: "99000",
        nticks: 6,
        title: {
          text: "Time",
          font: {
            size: 25,
            color: "#000000",
          },
        },
      },
      yaxis: {
        title: {
          text: "Amplitude",
          font: {
            size: 25,
            color: "#000000",
          },
        },
      },
      yaxis2: {
        title: {
          text: "DCCT Amplitude",
          font: {
            size: 25,
            color: "#000000",
          },
        },
        overlaying: "y",
        side: "right",
      },
      legend: {
        x: 1.05,
        y: 1,
        bgcolor: "#E2E2E2",
        bordercolor: "#FFFFFF",
        borderwidth: 2,
      },
    };
    plot_cnt_div.removeChild(document.getElementById("ps-1"));
    Plotly.newPlot("plot-container", data, layout);
    //og_range1 = document.getElementById("plot-container").layout.yaxis.range;
    //og_range2 = document.getElementById("plot-container").layout.yaxis2.range;
    document.getElementById("derived-metrics").replaceChildren();
        for (cn = 0; cn < selected_filters.channel_list.length; cn++) {
          metrics_div_maker(selected_filters.channel_list[cn]);
        }
        axios
          .post("/get_metrics", selected_filters, {}, config)
          .then((response) => {
            for (cn = 0; cn < selected_filters.channel_list.length; cn++) {
              create_list_of_metrics(
                response.data["metrics"][cn],
                selected_filters.channel_list[cn]
              );
            }
          });
  });
}

//end of functions making network calls

//window load event

window.addEventListener("load", function () {
  let plot_cnt_div = document.getElementById("plot-container");
  plot_cnt_div.appendChild(create_spinner("primary", "ps-1"));
  document.getElementById("zoom").value = "100";

  axios.post("/on_load", {}, config).then((response) => {
    let resp_data = response.data;
    set_calendar(resp_data["file_date"]);
    set_filter_3(resp_data["file_name"]);

    let x = resp_data["arr_x"];
    let y = resp_data["arr_y"];
    let y_dcct = resp_data["arr_dcct"];
    let data = [
      {
        x: x,
        y: y,
        type: "scatter",
        name: "LS1Q1D REF",
      },
      {
        x: x,
        y: y_dcct,
        type: "scatter",
        name: "DCCT",
        yaxis: "y2",
      },
    ];
    chart_data = data;
    let layout = {
      //autosize: false,
      width: 1300,
      height: 600,
      xaxis: {
        tickmode: "linear",
        tick0: "0",
        dtick: "99000",
        nticks: 6,
        title: {
          text: "Time",
          font: {
            size: 25,
            color: "#000000",
          },
        },
      },
      yaxis: {
        title: {
          text: "Amplitude",
          font: {
            size: 25,
            color: "#000000",
          },
        },
      },
      yaxis2: {
        title: {
          text: "DCCT Amplitude",
          font: {
            size: 25,
            color: "#000000",
          },
        },
        overlaying: "y",
        side: "right",
      },
      legend: {
        x: 1.05,
        y: 1,
        bgcolor: "#E2E2E2",
        bordercolor: "#FFFFFF",
        borderwidth: 2,
      },
    };
    chart_layout = layout;
    plot_cnt_div.removeChild(document.getElementById("ps-1"));
    Plotly.newPlot("plot-container", data, layout);
    og_range1 = document.getElementById("plot-container").layout.yaxis.range;
    og_range2 = document.getElementById("plot-container").layout.yaxis2.range;
    load_available_files(selected_filters);
    metrics_div_maker(1);
        metrics_div_maker(77);
        axios.post("/get_metrics_on_load", {}, config).then((response) => {
          create_list_of_metrics(response.data['channel_1'], 1);
          create_list_of_metrics(response.data['channel_77'], 77);
    
        });
  });
});

//end of window load event

//the filters selection events

//create channel dropbox
create_channel_checkbox();

//event selection filter event
let firstFilterList = document.getElementsByClassName("filter-1");
for (i = 0; i < firstFilterList.length; i++) {
  firstFilterList[i].addEventListener("click", function () {
    el = document.getElementById(this.id);
    //alert(el.innerHTML)
    set_filter_1(el.innerHTML.trim());
  });
}

//calendar date selection filter
document.getElementById("calendar").addEventListener("input", function () {
  set_calendar(this.value);
  load_available_files(selected_filters);
});

document.getElementById("rarr").addEventListener("click", function () {
  //alert(selected_filters["Event_Date"]);
  let date = new Date(selected_filters["Event_Date"]);
  date.setDate(date.getDate() + 1);
  // Format the updated date back into "yyyy-mm-dd" format
  date = date.toISOString().split("T")[0];
  set_calendar(date);
  load_available_files(selected_filters);
});

document.getElementById("larr").addEventListener("click", function () {
  //alert(selected_filters["Event_Date"]);
  let date = new Date(selected_filters["Event_Date"]);
  date.setDate(date.getDate() - 1);
  // Format the updated date back into "yyyy-mm-dd" format
  date = date.toISOString().split("T")[0];
  set_calendar(date);
  load_available_files(selected_filters);
});

//plot button
let plot_btn = document.getElementById("submit_btn");
plot_btn.addEventListener("click", function () {
  create_plot_function(selected_filters);
});

/*document.getElementById("zoom").addEventListener("input", function () {
  let range_1 = og_range1;
  let range_2 = og_range2;
  let range1_low;
  let range1_high;
  let range2_low;
  let range2_high;
  val = document.getElementById("zoom").value;
  if (val === "4") {
    range1_low = range_1[0];
    range1_high = range_1[1];
    range2_low = range_2[0];
    range2_high = range_2[1];
  } else {
    times = Number(5 - val);
    range1_low = Math.floor(range_1[0]);
    range1_high = Math.floor(range_1[1]);
    range2_low = Math.floor(range_2[0]);
    range2_high = Math.floor(range_2[1]);
    //range1_low--;
    range2_low = 0;
    for (i = 0; i < times; i++) {
      //range1_high += 0.025;
      range2_high = 10;
    }
  }
  const updated_layout = {
    ...chart_layout,
    yaxis: {
      ...chart_layout.yaxis,
      autorange: false,
      //range: [range1_low, range1_high],
    },
    yaxis2: {
      ...chart_layout.yaxis2,
      autorange: false,
      range: [range2_low, range2_high],
    },
  };
  chart_layout = updated_layout;
  //alert(`${range1_high}, ${range1_low}, ${range2_high}, ${range2_low}`);
  console.log(chart_layout);
  console.log("abc");
  Plotly.newPlot("plot-container", chart_data, updated_layout);
});*/
//end of filter selection events