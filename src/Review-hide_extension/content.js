'use strict';

// chrome.runtime.onInstalled.addListener(function() {
//   chrome.storage.sync.set({color: '#3aa757'}, function() {
//     console.log("The color is green.");
//   });

//   chrome.declarativeContent.onPageChanged.removeRules(undefined, function() {
//       chrome.declarativeContent.onPageChanged.addRules([{
//         conditions: [new chrome.declarativeContent.PageStateMatcher({
//           pageUrl: {hostEquals: 'www.amazon.com'},
//         })
//         ],
//             actions: [new chrome.declarativeContent.ShowPageAction()]
//       }]);
//   });
// });



$(document).ready(function() {

  var users =

  // for each input for listener:
       // I'm just going to assume that people with high average reviews aren't
       // the same as those with low ones, and do just two conditionals. I.e. I'm not
       // going to unhide/hide depending on multiple criteria, just update depending on
       // The most recent input.
  //   if clicked off then step through and turn back on

  chrome.runtime.onMessage.addListener(
    function(request, sender, sendResponse) {
      // console.log(sender.tab ?
      //             "from a content script:" + sender.tab.url :
      //             "from the extension");
      // alert(request.selector + ' ' + request.val);
      update(request.selector, request.val);
      sendResponse({farewell: "nope"});
      return true;
    }
  );

  function update(which_selector, val) {
    var reviewers = new Array(); // This will be turned into JSON and sent to the redis interface.
    $( "a.a-profile" ).each(function( idx ) {
      var secondidx = $(this).attr('href').search('/ref'); // second index of the substring containing the user id
      var user_id = $(this).attr('href').substring(26, secondidx); // it always starts at 25
      reviewers.push(user_id);
      if ($(this).attr('href').includes("AGLHTJEJHQQ763RAURZ2SRP2VKBA")) {
        $(this).parent().parent().css("display", "none");
      }
    });
  }

  function get_url() {
    var query_string = '?';
    var idx = 0;
    $( "a.a-profile" ).each(function() {
      var secondidx = $(this).attr('href').search('/ref'); // second index of the substring containing the user id
      var user_id   = $(this).attr('href').substring(26, secondidx); // it always starts at 25
      query_string += ('id' + idx + '=' + user_id + '&');
      idx++;
    });
    return ('https://storystreetconsulting.com/wsgi' + query_string);
  }

  $.ajax({
      dataType: 'json',
      url: get_url(),
      success: function(data) {
        update()
          console.log(JSON.stringify(data));


      },
      error: function(xhr, status, errorMsg) {
          $("#results").append("error");
          console.log(errorMsg);
      }
  });



  // var xhr = new XMLHttpRequest();
  // xhr.open("POST", 'https://www.storystreetconsulting.com/wsgi', true);
  // xhr.setRequestHeader('Content-Type', 'application/json');
  // xhr.send(JSON.stringify({
  //   value: reviewers
  // }));
  // console.log(JSON.stringify({
  //   value: reviewers
  // }));
});

// chrome.runtime.onMessage.addListener(
//   function(request, sender, sendResponse) {
//     console.log(sender.tab ?
//                 "from a content script:" + sender.tab.url :
//                 "from the extension");
//     alert(request.greeting);
//     sendResponse({farewell: "goodbye"});
//     // return true;
//   }
// );
