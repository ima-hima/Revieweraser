// Copyright 2018 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

'use strict';


let changeColor = document.getElementById('changeColor');
chrome.storage.sync.get('color', function(data) {
  changeColor.style.backgroundColor = data.color;
  changeColor.setAttribute('value', data.color);
});
changeColor.onclick = function(element) {
  let color = element.target.value;
  chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
    chrome.tabs.executeScript(
        tabs[0].id,
        {code: 'document.body.style.backgroundColor = "' + color + '";'});
  });
};
var minSlider = document.getElementById("min");
var minOutput = document.getElementById("minVal");
minVal.innerHTML = minSlider.value; // Display the default slider value

// Update the current slider value (each time you drag the slider handle)
minSlider.oninput = function() {
  minVal.innerHTML = this.value;
}

var maxSlider = document.getElementById("max");
var maxOutput = document.getElementById("maxVal");
maxVal.innerHTML = maxSlider.value; // Display the default slider value

// Update the current slider value (each time you drag the slider handle)
maxSlider.oninput = function() {
  maxVal.innerHTML = this.value;
}

var reviewers = document.getElementsByClassName("a-profile");

for (var i = 0, max = reviewers.length; i < max; i++) {
     if (reviewers[i].href.includes("AGLHTJEJHQQ763RAURZ2SRP2VKBA")) {
       reviewers[i].style.backgroundColor = "blue"
     }
}
