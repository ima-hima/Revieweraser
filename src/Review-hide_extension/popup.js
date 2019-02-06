// Copyright 2018 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

'use strict';

// First, functionality to hide sliders
// document.getElementById("averageReview").addEventListener("input", hideSliders); // This kludge because I couldn't get oninput to work.
var slideContainer = document.getElementById("slidecontainer");
var minSlider = document.getElementById("minSlider");
var minOutput = document.getElementById("curMinVal");
let maxSlider = document.getElementById("maxSlider");
let maxOutput = document.getElementById("curMaxVal");



function hideSliders() {
  // Hide the sliders whenever the "based on average review" is unchecked
  if (this.checked) {
    slideContainer.style.display = "block";
  } else {
    slideContainer.style.display = "none";
  }
}

// Now updated the slider values
// First with min value

minOutput.innerHTML = minSlider.value; // Display the default slider value

minSlider.oninput = function() {
  // Show min value in display
  minOutput.innerHTML = this.value;
  // chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
  //   chrome.tabs.sendMessage(tabs[0].id, {greeting: "hello"}, function(response) {
  //     console.log(response.farewell);
  //   });
  // });
}

// Same thing, now with max value
maxOutput.innerHTML = maxSlider.value;

maxSlider.oninput = function() {
  maxOutput.innerHTML = this.value;
  // chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
  //   chrome.tabs.sendMessage(tabs[0].id, {greeting: "hello"}, function(response) {
  //     console.log(response.farewell);
  //   });
  // });
}



//   chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
//   chrome.tabs.sendMessage(tabs[0].id, {greeting: "hello"}, function(response) {
//     console.log(response.farewell);
//   });
// });

