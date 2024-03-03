import React, { useState, useEffect } from "react";
import moment from "moment";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";
import "react-date-range/dist/styles.css";
import "react-date-range/dist/theme/default.css";
// import { Button } from "@mui/material";

import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

import axios from "axios";

function Chart() {
  // State Variables
  const [data, setData] = useState([]);
  const [view, setView] = useState("chart"); // 'chart' or 'table'
  const [individual, setIndividual] = useState(true);
  const [startDate, setStartDate] = useState(new Date());
  const [endDate, setEndDate] = useState(new Date());
  const [minDate, setMinDate] = useState(null);
  const [maxDate, setMaxDate] = useState(null);
  const [filteredData, setFilteredData] = useState(data);

  // Fetch data from the backend (Would be a EC2 instance in production)
  const fetchData = async () => {
    try {
      const response = await axios.get("http://localhost:8000/data/");
      setData(response.data);
      setFilteredData(response.data);

      if (response.data.length > 0) {
        const dates = response.data.map((item) => new Date(item.index));
        const oldestDate = new Date(Math.min(...dates));
        const mostRecentDate = new Date(Math.max(...dates));

        setStartDate(oldestDate);
        setEndDate(mostRecentDate);
        setMinDate(oldestDate);
        setMaxDate(mostRecentDate);
      } else {
        setMinDate(null);
        setMaxDate(null);
      }
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  const filterData = () => {
    const filtered = data.filter((item) => {
      const itemDate = new Date(item.index);
      return itemDate >= startDate && itemDate <= endDate;
    });
    setFilteredData(filtered);
  };

  // Update data for individual vs citywide
  const updateData = () => {
    if (individual) {
      return data;
    } else {
      const updatedData = data.map((item) => {
        return {
          ...item,
          kwh: item.kwh * 5567,
          predictions: item.predictions * 5567,
        };
      });
      return updatedData;
    }
  };

  // UseEffect (Update filtered data when date range changes)
  useEffect(() => {
    setFilteredData(updateData());
  }, [individual]);

  // UseEffect (Fetch data from the backend on load)
  useEffect(() => {
    fetchData();
  }, []);

  const formatDate = (timestamp) => {
    // Direct parsing
    let formattedDate = moment(timestamp).format("DD MMM YYYY");
    console.log("Formatted date:", formattedDate);
    return formattedDate;
  };

  // Custom tooltip for kWh
  const CustomTooltipKWH = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const kwh = payload[0].value;
      const dateLabel = payload[0].payload.index;
      const formattedDate = formatDate(dateLabel);

      return (
        <div
          className="custom-tooltip"
          style={{
            backgroundColor: "rgba(255, 255, 255, 0.9)",
            padding: "5px 10px",
            border: "1px solid #ccc",
            borderRadius: "10px",
            boxShadow: "0px 0px 5px 0px rgba(0,0,0,0.2)",
            color: "#333",
            fontSize: "14px",
            textAlign: "center",
            whiteSpace: "nowrap",
          }}
        >
          <p style={{ margin: "0" }}>Date: {formattedDate}</p>
          <p style={{ margin: "0" }}>
            Hour: {moment(dateLabel).format("HH:mm")}
          </p>
          <p style={{ margin: "0" }}>
            <span style={{ color: "#8884d8" }}>•</span>{" "}
            {`kWh: ${kwh.toLocaleString()}`}
          </p>
        </div>
      );
    } else {
      console.log("CustomTooltipKWH returning null");
    }
    return null;
  };

  // Custom tooltip for temperature
  const CustomTooltipTemp = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const temperature = payload[0].value;
      const dateLabel = payload[0].payload.index;
      const formattedDate = formatDate(dateLabel);

      return (
        <div
          className="custom-tooltip"
          style={{
            backgroundColor: "rgba(255, 255, 255, 0.9)",
            padding: "5px 10px",
            border: "1px solid #ccc",
            borderRadius: "10px",
            boxShadow: "0px 0px 5px 0px rgba(0,0,0,0.2)",
            color: "#333",
            fontSize: "14px",
            textAlign: "center",
            whiteSpace: "nowrap",
          }}
        >
          <p style={{ margin: "0" }}>Date: {formattedDate}</p>
          <p style={{ margin: "0" }}>
            Hour: {moment(dateLabel).format("HH:mm")}
          </p>
          <p style={{ margin: "0" }}>{`Temperature: ${temperature}`}</p>
        </div>
      );
    }

    return null;
  };

  return (
    <>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div style={{ backgroundColor: "#282c34" }}>
          <h1 style={{ color: "white", textAlign: "center" }}>
            Energy Consumption Prediction
          </h1>
        </div>

        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            padding: "20px",
            background: "#303846",
            borderRadius: "8px",
            boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
            alignItems: "center",
            width: "100%",
            marginBottom: "20px",
          }}
        >
          <div style={{ margin: "0 10px" }}>
            <p style={{ margin: "0 0 5px 0", fontWeight: "bold" }}>
              Select Start Date:
            </p>
            <DatePicker
              selected={startDate}
              onChange={(date) => setStartDate(date)}
              minDate={minDate}
              maxDate={maxDate}
              disabled={!minDate || !maxDate}
              style={{
                padding: "10px",
                border: "1px solid #ced4da",
                borderRadius: "4px",
                width: "100%",
              }}
            />
          </div>
          <div style={{ margin: "0 10px" }}>
            <p style={{ margin: "0 0 5px 0", fontWeight: "bold" }}>
              Select End Date:
            </p>
            <DatePicker
              selected={endDate}
              onChange={(date) => setEndDate(date)}
              minDate={minDate}
              maxDate={maxDate}
              disabled={!minDate || !maxDate}
              style={{
                padding: "10px",
                border: "1px solid #ced4da",
                borderRadius: "4px",
                width: "100%",
              }}
            />
          </div>
          <button
            onClick={filterData}
            disabled={!minDate || !maxDate}
            style={{
              padding: "10px 20px",
              border: "none",
              backgroundColor: "#007bff",
              color: "white",
              borderRadius: "4px",
              cursor: "pointer",
              fontWeight: "bold",
              textTransform: "uppercase",
              boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
            }}
          >
            Filter Data
          </button>
        </div>

        {/* Conditionally render the chart or table based on the view state */}
        <div
          style={{
            width: "100%",
            height: "100%",
            backgroundColor: "#282c34",
            padding: "20px",
            borderRadius: "10px",
            border: "1px solid #ccc",
            marginBottom: "20px",
          }}
        >
          {/* Title (Individual vs Citywide) */}
          <h2 style={{ color: "white" }}>
            {individual ? "Individual" : "Citywide"} Energy Consumption
          </h2>

          <div
            style={{
              display: "flex",
              justifyContent: "start",
              alignItems: "center",
              marginBottom: "20px",
              marginTop: "20px",
              fontSize: "18px",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                marginRight: "20px",
                fontSize: "14px",
              }}
            >
              {" "}
              {/* Encapsulate label and select in flex container */}
              <label
                htmlFor="dataScopeSelect"
                style={{
                  marginRight: "10px",
                  fontWeight: "bold",
                  fontSize: "14px",
                }}
              >
                Population:
              </label>
              <select
                id="dataScopeSelect"
                value={individual ? "individual" : "citywide"}
                onChange={(e) => setIndividual(e.target.value === "individual")}
                style={{
                  padding: "8px",
                  minWidth: "160px",
                  border: "1px solid #ced4da",
                  borderRadius: "4px",
                  cursor: "pointer",
                  outline: "none",
                  fontSize: "14px",
                }}
              >
                <option value="individual">Individual Data</option>
                <option value="citywide">Citywide Data</option>
              </select>
            </div>

            <div style={{ display: "flex", alignItems: "center" }}>
              {" "}
              {/* Second label-select pair in flex container */}
              <label
                htmlFor="viewSelect"
                style={{
                  marginRight: "10px",
                  fontWeight: "bold",
                  fontSize: "14px",
                }}
              >
                View:
              </label>
              <select
                id="viewSelect"
                value={view}
                onChange={(e) => setView(e.target.value)}
                style={{
                  padding: "8px",
                  minWidth: "120px",
                  border: "1px solid #ced4da",
                  borderRadius: "4px",
                  cursor: "pointer",
                  outline: "none",
                  fontSize: "14px",
                }}
              >
                <option value="table">Table</option>
                <option value="chart">Chart</option>
              </select>
            </div>
          </div>

          {view === "chart" ? (
            <>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-around",
                  alignItems: "flex-start",
                  marginTop: "30px",
                  marginBottom: "20px",
                }}
              >
                <LineChart
                  width={1000}
                  height={500}
                  data={filteredData}
                  margin={{ top: 5, right: 30, left: 30, bottom: 10 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <Line
                    type="monotone"
                    dataKey="kwh"
                    stroke="#9999ff"
                    strokeWidth={2}
                    dot={{ r: 1, fill: "#fff" }}
                  />
                  <Line
                    type="monotone"
                    dataKey="predictions"
                    stroke="#82ca9d"
                    strokeWidth={2}
                    dot={{ r: 1, fill: "#fff" }}
                    strokeDasharray="5 5"
                  />

                  <XAxis
                    dataKey="index"
                    tickFormatter={formatDate}
                    angle={-45}
                    textAnchor="end"
                    fontSize={14}
                    style={{ fill: "white" }}
                    dy={10}
                  />
                  <YAxis
                    label={{
                      value: "kWh",
                      angle: -90,
                      position: "insideLeft",
                      fill: "white",
                      dy: 10,
                      dx: -20,
                    }}
                  />

                  <Tooltip content={<CustomTooltipKWH />} />
                  <Legend />
                </LineChart>
              </div>

              <div
                style={{
                  display: "flex",
                  justifyContent: "space-around",
                  alignItems: "flex-start",
                  marginTop: "20px",
                }}
              >
                <LineChart
                  width={1000}
                  height={500}
                  data={filteredData}
                  margin={{ top: 5, right: 30, left: 30, bottom: 30 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <Line
                    type="monotone"
                    dataKey="temperature"
                    stroke="#82ca9d"
                    strokeWidth={2}
                    dot={{ r: 1, fill: "#fff" }}
                  />
                  <XAxis
                    dataKey="index"
                    tickFormatter={formatDate}
                    angle={-45}
                    textAnchor="end"
                    fontSize={14}
                    style={{ fill: "white" }}
                    dy={10}
                  />
                  <YAxis
                    label={{
                      value: "Temperature",
                      angle: -90,
                      position: "insideLeft",
                      fill: "white",
                      dy: 30,
                      dx: -20,
                    }}
                  />

                  {/* <Tooltip /> */}
                  <Tooltip content={<CustomTooltipTemp />} />
                  <Legend />
                </LineChart>
              </div>
            </>
          ) : (
            <div style={{ overflowY: "auto", height: "100%", width: "100%" }}>
              <div
                style={{
                  overflowY: "auto",
                  marginLeft: "20px",
                  width: "100%",
                  maxHeight: "750px",
                }}
              >
                <table className="table-style">
                  <thead>
                    <tr>
                      <th>Date</th>

                      <th>Recorded kWh</th>
                      <th>Weekday</th>
                      <th>Hour</th>
                      <th>Temperature</th>
                      <th>Holiday</th>
                      <th>Predicted kWh</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredData.map((item, index) => (
                      <tr key={index}>
                        <td>
                          {formatDate(item.index)}{" "}
                          {moment(item.index).format("HH:mm")}
                        </td>
                        <td>{item.kwh ? <td>{item.kwh}</td> : <td>N/A</td>}</td>
                        <td>{item.weekday}</td>
                        <td>{item.hour}</td>
                        <td>{item.temperature}</td>
                        <td>{item.holiday}</td>
                        <td>
                          {item.predictions ? (
                            <td>{item.predictions}</td>
                          ) : (
                            <td>N/A</td>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

export default Chart;
