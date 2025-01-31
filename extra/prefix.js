'use strict';

var tslib_1 = require('tslib');

var hasOwnProp = require('has-own-prop');
var toArray = require('stream-to-array');

var readableStream = require('readable-stream');
var Readable = readableStream.Readable;
var Writable = readableStream.Writable;
var Transform = readableStream.Transform;
var PassThrough = readableStream.PassThrough;

var immutableClass = require('immutable-class');
var generalEqual = immutableClass.generalEqual;
var generalLookupsEqual = immutableClass.generalLookupsEqual;
var isImmutableClass = immutableClass.isImmutableClass;
var immutableEqual = immutableClass.immutableEqual;
var immutableArraysEqual = immutableClass.immutableArraysEqual;
var immutableLookupsEqual = immutableClass.immutableLookupsEqual;
var SimpleArray = immutableClass.SimpleArray;
var NamedArray = immutableClass.NamedArray;

var Chronoshift = require('@topgames/chronoshift');
var Timezone = Chronoshift.Timezone;
var Duration = Chronoshift.Duration;
var moment = Chronoshift.moment;
var isDate = Chronoshift.isDate;
var parseISODate = Chronoshift.parseISODate;

var dummyObject = {};

