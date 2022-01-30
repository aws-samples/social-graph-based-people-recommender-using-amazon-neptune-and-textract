#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import pretty_errors

import aws_cdk as cdk
from octember_bizcard.octember_bizcard_stack import OctemberBizcardStack

app = cdk.App()
OctemberBizcardStack(app, "octember-bizcard")

app.synth()
